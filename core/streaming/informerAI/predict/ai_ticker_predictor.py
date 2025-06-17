class AITickerPredictor(Predictor):
    """
    Predictor class for the Informer model.
    """

    def __init__(self, args: Namespace, data_processor: DataProcessor):
        """
        Initializes the InformerPredictor.

        Args:
            args (Namespace): Arguments for the Informer model.
            data_processor (DataProcessor): Data processor for preparing data.
        """
        self.args = args
        self.data_processor = data_processor
        self.model = self._build_model()
        self._load_model()

    def _build_model(self) -> nn.Module:
        """
        Builds the Informer model.

        Returns:
            nn.Module: The built Informer model.
        """
        model = ActualInformerModel(self.args)
        logging.info("Informer model built successfully.")
        return model

    def _load_model(self):
        """
        Loads the pre-trained model weights.
        """
        model_path = os.path.join(self.args.checkpoints, 'checkpoint.pth')
        if not os.path.exists(model_path):
            raise FileNotFoundError(
                f"Model checkpoint not found at {model_path}")

        try:
            self.model.load_state_dict(
                torch.load(model_path, map_location='cpu'))
            self.model.eval()  # Set model to evaluation mode
            logging.info(f"Model loaded successfully from {model_path}")
        except Exception as e:
            logging.error(f"Error loading model from {model_path}: {e}")
            raise

    def predict(self, data) -> torch.Tensor:
        """
        Performs prediction using the Informer model.

        Args:
            data: The input data for prediction.

        Returns:
            torch.Tensor: The prediction results.
        """
        if not isinstance(data, torch.Tensor):
            logging.error("Input data must be
