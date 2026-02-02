import torch

class Problem:
    
    def __init__(self, train_X, train_y, init=5, ref_point=None, bounds=None):
        self.train_X = train_X
        self.train_y = train_y
        self.dim = train_X.shape[-1]
        self.num_objectives = train_y.shape[-1]
        init = min(init, train_X.shape[0])
        if bounds is not None:
            self.X_bounds, self.Y_bounds = bounds
        else:
            maxs = []
            mins = []
            for i in range(train_X.shape[1]):
                maxs.append(max(train_X[:init, i]))
                mins.append(min(train_X[:init, i]))
            self.X_bounds = torch.tensor([mins, maxs])
            maxs = []
            mins = []
            for i in range(train_y.shape[1]):
                maxs.append(max(train_y[:init, i]))
                mins.append(min(train_y[:init, i]))
            self.Y_bounds = torch.tensor([mins, maxs])
        if ref_point is not None:
            self.ref_point = ref_point
        else:
            ref_point = [min(train_X[:init, 1]),min(train_y[:init, -1])]
            self.ref_point = torch.tensor(ref_point, dtype=torch.float64)    
        
