from gpytorch import ExactMarginalLogLikelihood
from matplotlib import pyplot as plt
import torch
import pandas as pd
import numpy as np
from argparse import ArgumentParser
from gpytorch.kernels import ScaleKernel, MaternKernel

from gpytorch.mlls.sum_marginal_log_likelihood import SumMarginalLogLikelihood

from botorch.utils.transforms import unnormalize, normalize
from botorch.models.gp_regression import SingleTaskGP
from botorch.models.model_list_gp_regression import ModelListGP
from botorch.models.transforms.outcome import Standardize
from botorch import fit_gpytorch_mll
from botorch.optim.optimize import optimize_acqf, optimize_acqf_list, optimize_acqf_discrete
from botorch.utils.sampling import sample_simplex
from botorch.acquisition.objective import GenericMCObjective
from botorch.utils.multi_objective.scalarization import get_chebyshev_scalarization
from botorch.acquisition.logei import qLogNoisyExpectedImprovement
from botorch.acquisition.multi_objective.logei import qLogExpectedHypervolumeImprovement, qLogNoisyExpectedHypervolumeImprovement
from botorch.utils.multi_objective.pareto import is_non_dominated
from botorch.utils.multi_objective.box_decompositions.dominated import DominatedPartitioning
from botorch.sampling.normal import SobolQMCNormalSampler
from botorch.utils.multi_objective.box_decompositions.non_dominated import FastNondominatedPartitioning
from botorch.acquisition.multi_objective.analytic import ExpectedHypervolumeImprovement
from problem import Problem
from models import TransparentModel, MixedFixModel
from normalization import QuantileGaussianNormalizer, ReferencePointSelector


MC_SAMPLES = 128
BATCH_SIZE = 1
NUM_RESTARTS = 10
RAW_SAMPLES = 512

class BO:
    def __init__(self, acquisition, fix_dimension, init=5, training_file=None, test_file=None, results_file=None, verbose_file=None, train_X=None, train_y=None, test_X=None, alpha=1, zero=False, reference_point=None, bounds=None):
        self.acquisition = acquisition
        self.results_file = results_file    
        self.fix_dimension = fix_dimension
        self.alpha = alpha
        self.zero = zero
        if training_file is not None:
            self.train_X, self.train_y = self.loadTrainData(training_file)
            self.test_X = self.loadTestData(test_file)
            self.train_Y_cat = torch.cat([self.train_X[...,-1].reshape(-1,1), self.train_y.reshape(-1, 1)],-1)
            # 将外部传入的 reference_point（原始空间：[compression_ratio, latency]）
            # 转换到与 train_X/train_Y_hat 一致的归一化空间：
            # - CR: 使用 1/CR（与 prepocess_input 一致）
            # - latency: 使用 (min_latency_in_init / latency)（与 prepocess_input 一致）
            if reference_point is not None and not torch.is_tensor(reference_point):
                init_ = min(init, self.train_Y_cat.shape[0])
                time_min = self.train_Y_cat[:init_, 1].min()
                ref_cr, ref_time = float(reference_point[0]), float(reference_point[1])
                if ref_cr <= 0 or ref_time <= 0:
                    raise ValueError(f"reference_point must be positive, got: {reference_point}")
                reference_point = torch.tensor([1.0 / ref_cr, time_min / ref_time], dtype=torch.float64)

            self.train_X, self.train_Y_hat, self.test_X = self.prepocess_input(self.train_X, self.train_Y_cat, self.test_X, init)

        else:
            self.train_X = torch.tensor(train_X)
            self.train_y = torch.tensor(train_y)
            self.test_X = test_X
            self.train_Y_cat = torch.cat([self.train_X[...,-1].reshape(-1,1), self.train_y.reshape(-1, 1)],-1)
            self.train_Y_hat = self.train_Y_cat
        # X_bounds, Y_bounds = bounds
        
        # print(self.train_Y_cat.max(0).values)
        # self.train_Y_hat, ref_ponit = self.getNormYReferencePoint(self.train_Y_cat, self.alpha, self.zero)
        # self.train_Y_hat = normalize(self.train_Y_cat, Y_bounds)
        self.problem = Problem(self.train_X, self.train_Y_hat, init, reference_point, bounds)
        self.verbose = verbose_file if verbose_file else None
        
    def prepocess_input(self, X, Y, X_te, init):
        train_ind, train_CR, train_time = X[:, 0], X[:, 1], Y[:, 1]
        test_ind, test_CR = X_te[:, 0], X_te[:, 1]
        train_CR, test_CR = 1/train_CR, 1/test_CR
        train_time = train_time[:init].min()/train_time
        return (torch.cat([train_ind.reshape(-1, 1), train_CR.reshape(-1, 1)], -1),\
            torch.cat([train_CR.reshape(-1, 1), train_time.reshape(-1, 1)], -1),\
            torch.cat([test_ind.reshape(-1, 1), test_CR.reshape(-1, 1)], -1),)
        
        
    def getNextRecommendPoint(self, return_frontier) -> np.ndarray:
        if return_frontier:
            return self.getBest()
        mll, model = self.initialize_model()
        self.model = model
        fit_gpytorch_mll(mll)
        sampler = SobolQMCNormalSampler(sample_shape=torch.Size([MC_SAMPLES]))
        new_x = self.optimize(sampler)
        
        train_Y = self.train_Y_hat
        # 过滤掉小于 reference_point 的点，确保超体积计算的鲁棒性
        diff = train_Y - self.problem.ref_point
        valid_mask = (diff > 0).all(dim=1)
        if valid_mask.any():
            # 只使用所有目标都优于 reference_point 的点来计算超体积
            valid_Y = train_Y[valid_mask]
            bd = DominatedPartitioning(ref_point=self.problem.ref_point, Y=valid_Y)
            volume = bd.compute_hypervolume().item()
        else:
            # 如果没有有效点，超体积为 0
            volume = 0.0
        if self.verbose is not None:
            with open(self.verbose, "a") as f:
                f.write(f"new x: "+ str(new_x.numpy())+ " Pred Volume: "+ str(volume)+"\n")
        # print("new x: "+ str(new_x.numpy())+ ", Pred Volume: "+ str(volume))
        return new_x

    def getBest(self):
        # max_volumne = -float("inf")
        # #orginal_Y = self.train_Y_hat * (self.problem.ref_point) * 2
        # best_idx = 0
        # for(idx, y) in enumerate(self.train_Y_hat):
        #     diff = self.train_Y_hat[idx]-self.problem.ref_point
        #     if (diff<=0).any():
        #         continue
        #     volumne = diff[0]*diff[1]
        #     if volumne > max_volumne:
        #         max_volumne = volumne
        #         best_idx = idx
        # return np.concatenate([self.train_X[best_idx:best_idx+1, :], self.train_Y_cat[best_idx:best_idx+1, :]],axis=1)
        # 使用 train_Y_hat 而不是 train_Y_cat，因为 ref_point 是基于 train_Y_hat 计算的
        diff = self.train_Y_hat - self.problem.ref_point
        # Only consider points where all objectives are better than reference point
        valid_mask = (diff > 0).all(dim=1)
        if not valid_mask.any():
            # If no valid point, return the point with minimum distance to reference point
            distances = torch.norm(diff, dim=1)
            best_idx = torch.argmin(distances)
        else:
            # Calculate hypervolume only for valid points
            hypervolume = diff[:, 0] * diff[:, 1]
            hypervolume = torch.where(valid_mask, hypervolume, torch.tensor(-float('inf'), dtype=hypervolume.dtype))
            best_idx = torch.argmax(hypervolume)
        return np.concatenate([self.train_X[best_idx:best_idx+1, :], self.train_Y_cat[best_idx:best_idx+1, :]],axis=1)
        
            
    def getNormYReferencePoint(self, train_Y, alpha, zero):
        # CR -> Compression rate
        # latency (ms) -> qps (1000/latency)
        # train_Y_ = np.concatenate([
        #     1 / train_Y[:, 0].reshape(-1, 1),
        #     1000 / train_Y[:, 1].reshape(-1, 1)
        # ], axis=-1) 
        train_Y_ = train_Y
        normalizer = QuantileGaussianNormalizer()
        normed = normalizer.transform(train_Y_)
        # if zero:
        #     ref_point = torch.tensor([0, 0], dtype=torch.float64)
        # else:
        ref_point = ReferencePointSelector(alpha=alpha).select(normed)
        return torch.tensor(normed, dtype=torch.float64), torch.tensor(ref_point, dtype=torch.float64)

    def getParetoFront(self):
        # get the pareto front of all the training data
        frontier = []
        train_Y = self.train_Y_hat
        dom = is_non_dominated(train_Y.unsqueeze(0))
        for i, x in enumerate(self.train_X):
            if dom[0, i]:
                # add to the pareto front
                frontier.append(x)
        return np.array(frontier), train_Y.unsqueeze(0)
                
    def optimize(self, sampler):
        train_X = normalize(torch.tensor(self.train_X, dtype=torch.float64), self.problem.X_bounds)
        test_X = normalize(self.test_X, self.problem.X_bounds)
        if self.acquisition == "qehvi":
            with torch.no_grad():
                pred = self.model.posterior(train_X).mean
            partitioning = FastNondominatedPartitioning(
                ref_point=self.problem.ref_point,
                Y=pred,
            )
            acq_func = qLogExpectedHypervolumeImprovement(
                model=self.model,
                ref_point=self.problem.ref_point,
                partitioning=partitioning,
                sampler=sampler,
            )
            evhi = ExpectedHypervolumeImprovement(
                model=self.model,
                ref_point=self.problem.ref_point,
                partitioning=partitioning,
            )
            test_pred = self.model.posterior(test_X)
            mean = test_pred.mean.detach()[:, -1].flatten()
            std = test_pred.variance.sqrt().detach()[:, -1].flatten()
            unorm_X_test = unnormalize(test_X, bounds=self.problem.X_bounds)
            unorm_X_train = unnormalize(train_X, bounds=self.problem.X_bounds)
            # plt.cla()
            # plt.plot(test_X[:, 0].flatten(), mean, label="GP mean")
            # plt.fill_between(
            #     test_X[:, 0].flatten(),
            #     (mean - 2 * std),
            #     (mean + 2 * std),
            #     alpha=0.3,
            #     label="95% CI"
            # )
            # plt.scatter(train_X[:, 0].flatten(), self.train_Y_hat[:, -1].flatten(), c="r", label="Train points")
            # plt.legend()
            # plt.xlim(test_X[:,0].min()-0.1, test_X[:,0].max()+0.1)
            # plt.show()
            # # save fig
            # plt.savefig("test_pred.png", dpi=300)
            # optimize
            candidates, _ = optimize_acqf_discrete(
                    acq_function=evhi,
                    q=BATCH_SIZE,
                    choices=test_X,
                    X_avoid=train_X,
                    unique=True,
            )
            # observe new values
            new_x = unnormalize(candidates.detach(), bounds=self.problem.X_bounds)
            # new_x = torch.cat([new_x[0], 1/new_x[1]], -1)
            new_x[0, 1] = 1/new_x[0, 1]
            return new_x
        elif self.acquisition == "qnehvi":
            acq_func = qLogNoisyExpectedHypervolumeImprovement(
                model=self.model,
                ref_point=self.problem.ref_point.tolist(),  # use known reference point
                X_baseline=train_X,
                prune_baseline=True,  # prune baseline points that have estimated zero probability of being Pareto optimal
                sampler=sampler,
            )
            # optimize
            # candidates, _ = optimize_acqf(
            #     acq_function=acq_func,
            #     bounds=self.problem.bounds,
            #     q=BATCH_SIZE,
            #     num_restarts=NUM_RESTARTS,
            #     raw_samples=RAW_SAMPLES,  # used for intialization heuristic
            #     options={"batch_limit": 5, "maxiter": 200},
            #     sequential=True,
            # )
            # print(test_X, train_X)
            candidates, _ = optimize_acqf_discrete(
                    acq_function=acq_func,
                    q=BATCH_SIZE,
                    choices=test_X,
                    X_avoid=train_X,
            )
            # observe new values
            new_x = unnormalize(candidates.detach(), bounds=self.problem.X_bounds)
            print(new_x)
            new_x[0, 1] = 1/new_x[0, 1]
            return new_x
        else:
            raise ValueError(f"Unknown acquisition function: {self.acquisition}")
        # elif self.acquisition == "qnparego":
        #     train_x = normalize(train_X, self.problem.bounds)
        #     with torch.no_grad():
        #         pred = self.model.posterior(train_x).mean
        #     acq_func_list = []
        #     for _ in range(BATCH_SIZE):
        #         weights = sample_simplex(self.problem.num_objectives,).squeeze()
        #         objective = GenericMCObjective(
        #             get_chebyshev_scalarization(weights=weights, Y=pred)
        #         )
        #         acq_func = qLogNoisyExpectedImprovement(  # pyre-ignore: [28]
        #             model=self.model,
        #             objective=objective,
        #             X_baseline=train_x,
        #             sampler=sampler,
        #             prune_baseline=True,
        #         )
        #         acq_func_list.append(acq_func)
        #     # optimize
        #     candidates, _ = optimize_acqf_list(
        #         acq_function_list=acq_func_list,
        #         bounds=self.problem.bounds,
        #         num_restarts=NUM_RESTARTS,
        #         raw_samples=RAW_SAMPLES,  # used for intialization heuristic
        #         options={"batch_limit": 5, "maxiter": 200},
        #     )
        #     # observe new values
        #     new_x = unnormalize(candidates.detach(), bounds=self.problem.bounds)
        #     return new_x
    
    # def norm(self, X, dimension):
    #     return X/(self.problem.ref_point[dimension]*2)
    def norm(self, X, dimension):
        normalizer = QuantileGaussianNormalizer()
        normed = normalizer.transform_1(X)
        return torch.tensor(normed, dtype=torch.float64)
        
    def initialize_model(self):
        # define models for objective and constraint
        train_X = torch.tensor(self.train_X, dtype=torch.float64)
        train_x = normalize(train_X, self.problem.X_bounds)
        models = []
        # for i in range(self.train_y.shape[-1]):
        #     if i==self.fix_dimension:
        #         models.append(TransparentModel(self.fix_dimension))
        #     else:
        train_y = self.train_Y_hat[..., 1:].clone().detach()
        # train_yvar = torch.full_like(train_y, NOISE_SE[i] ** 2)
        # gp_model = SingleTaskGP(
        #         train_x, train_y,
        #         covar_module=ScaleKernel(MaternKernel(nu=0.5)),
        #         # , outcome_transform=Standardize(m=1)
        #     )
        from gpytorch.kernels import RBFKernel, ScaleKernel

        # 创建核函数
        rbf = RBFKernel(ard_num_dims=train_x.shape[-1])
        rbf.lengthscale = torch.tensor([[10] * train_x.shape[-1]], dtype=torch.float64)

        # 用 ScaleKernel 包裹它
        kernel = ScaleKernel(rbf)

        # 创建模型
        gp_model = SingleTaskGP(train_x, train_y, 
                               #  covar_module=kernel
                                )
        models.append(gp_model)
        fixed_model = TransparentModel(self.problem.X_bounds, unnormalize, self.norm, self.fix_dimension)
        models.append(fixed_model)
                
        model = MixedFixModel(gp_model, fixed_model, batch_shape=1, fixed_dimension=self.fix_dimension)
        # mll = SumMarginalLogLikelihood(model.likelihood, ModelListGP(model.model,))
        mll = ExactMarginalLogLikelihood(gp_model.likelihood, gp_model)
        return mll, model
    
    def loadTrainData(self, training_file):
        # Placeholder for loading training data
        train_df = pd.read_csv(training_file, header=None)
        train_X = torch.from_numpy(train_df.iloc[:, :-1].values)
        train_y = torch.from_numpy(train_df.iloc[:, -1].values.reshape(-1, 1))
        return train_X, train_y
    
    def loadTestData(self, test_file):
        # Placeholder for loading test data
        if test_file is None:
            return None
        test_df = pd.read_csv(test_file, header=None)
        test_X = test_df.values
        return torch.tensor(test_X, dtype=torch.float64)
    
    def saveResults(self):
        # Placeholder for saving results
        pred_mean = self.model.posterior(self.test_X).mean
        # concatenate the text_X and pred_mean
        results = np.concatenate([torch.cat([self.test_X[:, 0], 1/self.test_X[:, 1]], -1), pred_mean[:, -1]], axis=-1)
        results_df = pd.DataFrame(results, columns=["X"+str(i) for i in range(self.test_X.shape[-1])] + ["pred_mean"])
        results_df.to_csv("results.csv", index=False)
    
if __name__ == "__main__":
    # get the command line arguments
    parser = ArgumentParser(description="Bayesian Optimization")
    # parser.add_argument("--models", type=str, required=True, help="Models to use for optimization")
    parser.add_argument("--acquisition", type=str, required=True, help="Acquisition function to use")
    parser.add_argument("--train", type=str, required=True, help="Path to the training data file")
    parser.add_argument("--test", type=str, required=True, help="Path to the test data file")
    parser.add_argument("--fix_dimension", type=int, required=True, help="Fixed dimension for optimization")
    parser.add_argument("--results", type=str, required=True, help="Path to save the results")
    parser.add_argument("--verbose", type=str, help="Path to save the verbose output")
    parser.add_argument("--best", action="store_true", help="Return the pareto frontier")
    parser.add_argument("--init", type=int, help="Number of initial points", default=5)
    parser.add_argument(
        "--reference_point",
        type=float,
        nargs=2,
        default=None,
        help="Reference point in original space: <compression_ratio> <latency>",
    )
    args = parser.parse_args()
    bo = BO(
        args.acquisition,
        args.fix_dimension,
        args.init,
        args.train,
        args.test,
        args.results,
        args.verbose,
        reference_point=args.reference_point,
    )
    recommend_point = bo.getNextRecommendPoint(args.best)
    # write the recommend point to the results file
    print("writ to results file: "+ args.results)
    with open(args.results, "w") as f:
        for x in recommend_point:
            for x_i in x:
                f.write(f"{x_i}")
                if x_i != x[-1]:
                    f.write(",")
            f.write("\n")
    
    