import numpy as np
from scipy.stats import norm as gaussian_norm

class QuantileGaussianNormalizer:
    def __init__(self, lower_pct=0.01, upper_pct=0.99):
        """
        lower_pct, upper_pct: 用于 ECDF 截断的百分位边界，
        例如 0.01/0.99 分别对应 1% 和 99% 分位数。
        """
        self.lower_pct = lower_pct
        self.upper_pct = upper_pct

    def transform(self, train_Y):
        """
        对两个目标分别做 ECDF → 百分位截断 → Gaussian 归一化，
        不再选择参考点。

        参数:
            train_Y (np.ndarray): shape = (n_samples, 2)
                第一列: 压缩率倒数 (CR)
                第二列: 查询效率 (1/latency)

        返回:
            np.ndarray: 归一化后的数据，shape = (n_samples, 2)
        """
        if len(train_Y) == 1:
            # 如果只有一个样本，则直接返回
            return train_Y
        # 提取原始目标
        cr = np.log(train_Y[:, 0])
        qe = train_Y[:, 1]

        # # 对 CR 做 ECDF → 百分位截断 → Gaussian
        # sorted_cr = np.sort(cr)
        # u_cr = np.searchsorted(sorted_cr, cr, side="right") / len(cr)
        # u_cr = np.clip(u_cr, self.lower_pct, self.upper_pct)
        # cr_gauss = gaussian_norm.ppf(u_cr).reshape(-1, 1)

        # # 对 QE 做 ECDF → 百分位截断 → Gaussian
        # sorted_qe = np.sort(qe)
        # u_qe = np.searchsorted(sorted_qe, qe, side="right") / len(qe)
        # u_qe = np.clip(u_qe, self.lower_pct, self.upper_pct)
        # qe_gauss = gaussian_norm.ppf(u_qe).reshape(-1, 1)
            
        miu = cr.mean()
        sigma = cr.std()
        cr_gauss = (cr - miu) / sigma

        miu = qe.mean()
        sigma = qe.std()
        qe_gauss = (qe - miu) / sigma
        if len(cr_gauss.shape) == 1:
            cr_gauss = cr_gauss.reshape(-1, 1)
        if len(qe_gauss.shape) == 1:
            qe_gauss = qe_gauss.reshape(-1, 1)
            
        # 合并并返回
        return np.concatenate([cr_gauss, qe_gauss], axis=1)
    
    def transform_1(self, X):
        # only normalize the CR
        if len(X) == 1:
            return X
        shape = X.shape
        X = X.reshape(-1).log()
        # sorted_cr = np.sort(X)
        # u_cr = np.searchsorted(sorted_cr, X, side="right") / len(X)
        # u_cr = np.clip(u_cr, self.lower_pct, self.upper_pct)
        # cr_gauss = gaussian_norm.ppf(u_cr)
        
        miu = X.mean()
        sigma = X.std()
        cr_gauss = (X - miu) / sigma
        
        return cr_gauss.reshape(shape)

class ReferencePointSelector:

    def __init__(self, delta=0.005, alpha=1):
        self.delta = delta
        self.alpha = alpha

    def select(self, Z: np.ndarray) -> np.ndarray:
        # Compute per-objective minima and maxima
        z_min = np.min(Z, axis=0)
        z_max = np.max(Z, axis=0)

        # Compute range per objective
        ranges = z_max - z_min
        if self.alpha <= 1:
            ranges[0] = ranges[1] / self.alpha
        elif self.alpha >= 1:
            ranges[1] = ranges[0] * self.alpha
        # Reference point: nadir + delta * range
        ref_point = z_min - self.delta * ranges
        return ref_point

# Example usage:
# Z_norm = np.array([[0.1, -1.2], [0.5, 0.3], [0.2, 0.0]])
# selector = ReferencePointSelector(delta=0.1)
# ref = selector.select(Z_norm)
# print("Reference point:", ref)


def norm(train_Y, method='original'):
    # CR -> Compression rate
    # latency (ms) -> qps (1000/latency)
    train_Y_ = np.concatenate([
        1 / train_Y[:, 0].reshape(-1, 1),
        1000 / train_Y[:, 1].reshape(-1, 1)
    ], axis=-1) 
    normalizer = QuantileGaussianNormalizer()
    normed = normalizer.transform(train_Y_)
    ref_point = ReferencePointSelector().select(normed)
    return normed, ref_point

