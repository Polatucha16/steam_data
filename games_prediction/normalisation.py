from abc import ABC, abstractmethod
import numpy as np

class NormalisationStrategy(ABC):
    @abstractmethod
    def normalise(self, predictions_for_user):
        pass

class NormaliseRanks(NormalisationStrategy):

    def __init__(self,max_rank_value) -> None:
        self.max_rank_value = max_rank_value

    def normalise(self, predictions_for_user:np.ndarray, inds_to_predict:np.ndarray)->np.ndarray:
        normalised_predictions = np.copy(predictions_for_user)
        normalised_predictions[inds_to_predict] = self.max_rank_value * np.clip(
            predictions_for_user[inds_to_predict], a_min=0, a_max=np.inf)/np.max(predictions_for_user[inds_to_predict])
        return normalised_predictions