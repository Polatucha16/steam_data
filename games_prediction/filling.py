from abc import ABC, abstractmethod
import numpy as np

from csmc import CSMC

class FillingStrategy(ABC):
    @abstractmethod
    def fill(self, M_incomplete:np.ndarray):
        pass

class FillWithCSMC(FillingStrategy):

    def __init__(self, col_fraction=None)->None:
        self.col_fraction=col_fraction

    def fill(self, M_incomplete:np.ndarray) -> np.ndarray:
        
        if self.col_fraction == None:
            self.col_number = int(0.1 * M_incomplete.shape[-1])
        else:
            self.col_number = int(self.col_fraction * M_incomplete.shape[-1])
        solver = CSMC(M_incomplete, col_number=self.col_number)
        M_filled = solver.fit_transform(M_incomplete)
        return M_filled