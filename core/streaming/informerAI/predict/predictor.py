# autopep8: off
import findspark  # type: ignore
findspark.init()
from core.streaming.informerAI.models.data_processor import DataProcessor  # type: ignore
from abc import ABC, abstractmethod
from argparse import Namespace  # type: ignore
import logging, os, sys  # type: ignore

import torch
import torch.nn as nn
from torch.utils.data import DataLoader
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../../Informer2020"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the actual model class, not the experiment runner
from Informer2020.models.model import Informer as ActualInformerModel

# autopep8: on


class Predictor(ABC):
    """
    Abstract base class for predictors.
    """
    @abstractmethod
    def predict(self, data):
        """
        Abstract method to perform prediction.
        """
        pass
