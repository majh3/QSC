import torch
from typing import Optional, Union
from torch import Tensor, distributions, nn
from torch.distributions.distribution import Distribution
from botorch.acquisition.objective import PosteriorTransform
from botorch.models.model import Model
from botorch.posteriors.posterior import Posterior
from botorch.posteriors.torch import TorchPosterior
from botorch.models.gpytorch import GPyTorchModel
from gpytorch.distributions import MultitaskMultivariateNormal, MultivariateNormal
from botorch.posteriors.gpytorch import GPyTorchPosterior

  
class TransparentModel(GPyTorchModel):
    fixed_dimension: int
    def __init__(self, bounds, X_unnormalize_func, Y_normalize_func, fixed_dimension: int = 0):
        super(TransparentModel, self).__init__()
        self.fixed_dimension = fixed_dimension
        self.bounds = bounds
        self.unnormalize_func_x = X_unnormalize_func
        self.normalize_func_y = Y_normalize_func

    def posterior(
        self,
        X: Tensor,
        output_indices: Optional[list[int]] = None,
        observation_noise: Union[bool, Tensor] = False,
        posterior_transform: Optional[PosteriorTransform] = None,
    ) -> Posterior:
        # TorchPosterior directly wraps our torch.distributions.Distribution output.
        # posterior = TorchPosterior(distribution=FixedTensorDistribution(X[:, self.fixed_dimension].reshape(*X.shape[:-1], 1)))
        # return posterior
        # recover it from the GP model's normalized output
        # norm it to the normed Y
        X = self.unnormalize_func_x(X, self.bounds)
        # X_1 = 1/X[..., self.fixed_dimension].reshape(*X.shape[:-1])
        X_1 = X[..., self.fixed_dimension].reshape(*X.shape[:-1])
        # mean = self.normalize_func_y(X_1, 0)
        
        var_size = X.shape[-2]
        # if X.ndim==3:
        #     cov = cov.tile(X.shape[0], 1, 1)
        # print(mean.shape, cov.shape)
        return  GPyTorchPosterior(distribution=MultivariateNormal(
            X_1, torch.eye(var_size)*1.0e-18,
        ))

class MixedFixModel:
    def __init__(self, model: Model, fixed_model: TransparentModel, batch_shape, fixed_dimension: int = 0):
        self.model = model
        self.fixed_model = fixed_model
        self.likelihood = model.likelihood
        self.fixed_dimension = fixed_dimension
        self.batch_shape = batch_shape
        
    def posterior(
        self,
        X: Tensor,
        output_indices: Optional[list[int]] = None,
        observation_noise: Union[bool, Tensor] = False,
        posterior_transform: Optional[PosteriorTransform] = None,
    ) -> Posterior:
        # concatenate the two models posterior
        # get the posterior of the first model
        posterior = self.model.posterior(X, output_indices, observation_noise, posterior_transform).distribution
        # get the posterior of the second model
        fixed_posterior = self.fixed_model.posterior(X, output_indices, observation_noise, posterior_transform).distribution
        # create a new posterior
        # print("X.shape: ", X.shape)
        # print("gp mean: ", posterior.mean.shape, " gp cov: ", posterior.covariance_matrix.shape)
        # print("fixed gp mean: ", fixed_posterior.mean.shape, " fixed gp cov: ", fixed_posterior.covariance_matrix.shape)
        mvns = [fixed_posterior, posterior]
        multi_posterior = MultitaskMultivariateNormal.from_independent_mvns(mvns=self._broadcast_mvns(mvns=mvns))
        return GPyTorchPosterior(distribution=multi_posterior)

    def _broadcast_mvns(self, mvns: list[MultivariateNormal]) -> MultivariateNormal:
        """Broadcasts the batch shapes of the given MultivariateNormals.

        The MVNs will have a batch shape of `input_batch_shape x model_batch_shape`.
        If the model batch shapes are broadcastable, we will broadcast the mvns to
        a batch shape of `input_batch_shape x self.batch_shape`.

        Args:
            mvns: A list of MultivariateNormals.

        Returns:
            A list of MultivariateNormals with broadcasted batch shapes.
        """
        mvn_batch_shapes = {mvn.batch_shape for mvn in mvns}
        if len(mvn_batch_shapes) == 1:
            # All MVNs have the same batch shape. We can return as is.
            return mvns
        # This call will error out if they're not broadcastable.
        # If they're broadcastable, it'll log a warning.
        target_model_shape = self.batch_shape
        max_batch = max(mvn_batch_shapes, key=len)
        max_len = len(max_batch)
        input_batch_len = max_len - len(target_model_shape)
        for i in range(len(mvns)):  # Loop over index since we modify contents.
            while len(mvns[i].batch_shape) < max_len:
                # MVN is missing batch dimensions. Unsqueeze as needed.
                mvns[i] = mvns[i].unsqueeze(input_batch_len)
            if mvns[i].batch_shape != max_batch:
                # Expand to match the batch shapes.
                mvns[i] = mvns[i].expand(max_batch)
        return mvns