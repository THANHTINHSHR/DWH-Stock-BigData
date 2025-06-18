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


class Trainer(ABC):
    def __init__(self, data_processor: DataProcessor):
        self.data_processor = data_processor
        self.symbol_dict = data_processor.symbol_dict
        self.target_columns = data_processor.feature_cols
        self.len_target_columns = len(self.target_columns)

        self.BATCH_SIZE = data_processor.BATCH_SIZE
        self.SEQUENCE_LENGTH = data_processor.SEQUENCE_LENGTH
        self.PREDICTION_LENGTH = data_processor.PREDICTION_LENGTH
        self.NUM_EPOCHS = data_processor.NUM_EPOCHS

        self.config = {
            'enc_in': self.len_target_columns,
            'dec_in': self.len_target_columns,
            'c_out': self.len_target_columns,
            'seq_len': self.SEQUENCE_LENGTH,
            'label_len': self.SEQUENCE_LENGTH - self.PREDICTION_LENGTH,
            'pred_len': self.PREDICTION_LENGTH,
            'batch_size': self.BATCH_SIZE,
            'factor': 5,
            'd_model': 64,
            'n_heads': 8,
            'd_ff': 256,
            'e_layers': 2,
            'd_layers': 1,
            'dropout': 0.1,
            'attn': 'prob',
            'activation': 'gelu',
            'distil': True,
            'output_attention': False,
            'embed': 'timeF',  # !!!! FOR CONFIG TIME_COLS
            'freq': 's',    # !!!! FOR CONFIG TIME_COLS
            'lr': 1e-3  # or 5e-3 (0.05) may be risk!
        }
        # Model
        self.model = ActualInformerModel(  # Use the correctly imported model class
            # (input features)
            enc_in=self.config['enc_in'],
            dec_in=self.config['dec_in'],
            c_out=self.config['c_out'],
            seq_len=self.config['seq_len'],
            # Decode
            label_len=self.config['label_len'],
            out_len=self.config['pred_len'],

            # Embedding
            d_model=self.config.get('d_model', 64),
            # số attention heads
            n_heads=self.config.get('n_heads', 8),
            e_layers=self.config.get('e_layers', 2),
            d_layers=self.config.get('d_layers', 1),
            dropout=self.config.get('dropout', 0.1),

            # Attention: prob/sparse/full
            attn=self.config.get('attn', 'prob'),
            # Distillation encoder
            distil=self.config.get('distil', True),
            activation=self.config.get('activation', 'gelu'),
            # Attention weights
            output_attention=self.config.get('output_attention', False),
            # Embedding type
            embed=self.config.get('embed', 'timeF'),
            freq=self.config.get('freq', 's'),
            # cuda or cpu
            device=torch.device('cpu')
        )
        self.device = torch.device('cpu')

        self.criterion = nn.MSELoss()
        self.optimizer = torch.optim.Adam(
            self.model.parameters(), lr=self.config.get('lr', 1e-3))

        self.best_val_loss = float('inf')

        self.save_path = "./core/streaming/informerAI/files/models/"
        os.makedirs(self.save_path, exist_ok=True)

        self.logger = logging.getLogger(self.__class__.__name__)

    def get_data_loader_from_symbol(self, symbol) -> tuple[DataLoader, DataLoader, DataLoader]:
        train_ds, val_ds, test_ds = self.symbol_dict[symbol][
            "train"], self.symbol_dict[symbol]["val"], self.symbol_dict[symbol]["test"]

        train_loader = DataLoader(
            train_ds, batch_size=self.BATCH_SIZE, shuffle=True)
        val_loader = DataLoader(
            val_ds, batch_size=self.BATCH_SIZE, shuffle=False)
        test_loader = DataLoader(
            test_ds, batch_size=self.BATCH_SIZE, shuffle=False)
        return train_loader, val_loader, test_loader

    def train(self, tuple_loader: tuple[DataLoader, DataLoader, DataLoader], save_path='ticker_informer_best.pth'):
        self.logger.info("⏳ Training model...")
        train_loader, val_loader, test_loader = tuple_loader
        best_val_loss = float('inf')

        for epoch in range(1, self.NUM_EPOCHS + 1):
            self.logger.info(f"🚀 Epoch {epoch} Start")

            self.model.train()
            total_loss = 0

            for batch_idx, (x_enc, x_mark_enc, x_dec, x_mark_dec, y) in enumerate(train_loader):
                try:
                    x_enc = x_enc.to(self.device).float()
                    x_mark_enc = x_mark_enc.to(self.device).float()
                    x_dec = x_dec.to(self.device).float()
                    x_mark_dec = x_mark_dec.to(self.device).float()
                    y = y.to(self.device).float()

                    self.optimizer.zero_grad()
                    output = self.model(x_enc, x_mark_enc, x_dec, x_mark_dec)

                    y_for_loss = y.clone().detach()
                    loss = self.criterion(output, y_for_loss)
                    loss.backward()
                    self.optimizer.step()

                    total_loss += loss.item()

                except RuntimeError as e:
                    if "must match the size" in str(e):
                        self.logger.warning(
                            f"⚠️ Skip batch {batch_idx} due to size mismatch: {e}")
                        continue
                    self.logger.error(
                        f"❌ RuntimeError at batch {batch_idx}: {e}")
                    continue

                except Exception as e:
                    self.logger.error(f"❌ Error at batch {batch_idx}: {e}")
                    continue

            avg_loss = total_loss / \
                len(train_loader) if len(train_loader) > 0 else 0
            val_loss = self.evaluate(tuple_loader)

            self.logger.info(
                f"📌 Epoch {epoch:02d} | Train Loss: {avg_loss:.4f} | Val Loss: {val_loss:.4f}")

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                torch.save(self.model.state_dict(),
                           os.path.join(self.save_path, save_path))
                self.logger.info(
                    f"💾 Saved best model to {save_path} (Val Loss: {val_loss:.4f})")

    def evaluate(self, tuple_loader: tuple[DataLoader, DataLoader, DataLoader], model_path=None):
        train_loader, val_loader, test_loader = tuple_loader
        if model_path:
            self.model.load_state_dict(torch.load(
                model_path, map_location=self.device))
            self.model.eval()
            print(f"📂 Loaded model from: {model_path}")

        self.model.eval()
        total_loss = 0

        with torch.no_grad():
            for batch_idx, (x_enc, x_mark_enc, x_dec, x_mark_dec, y) in enumerate(val_loader):
                try:
                    x_enc = x_enc.to(self.device).float()
                    y_val = y.to(self.device).float()
                    x_mark_enc = x_mark_enc.to(self.device).float()
                    x_mark_dec = x_mark_dec.to(self.device).float()

                    dec_inp = torch.zeros_like(y_val).to(self.device)
                    # output = self.model(x_enc, x_mark_enc, dec_inp, x_mark_dec)
                    output = self.model(x_enc, x_mark_enc, x_dec, x_mark_dec)

                    loss = self.criterion(output, y_val)
                    total_loss += loss.item()
                except RuntimeError as e:
                    error_message = str(e)
                    if "The size of tensor a" in error_message and "must match the size of tensor b" in error_message:
                        self.logger.warning(
                            f"⚠️ Skipping validation batch {batch_idx} due to tensor size mismatch: {error_message}")
                        continue
                    else:
                        self.logger.error(
                            f"❌ Unhandled RuntimeError in validation batch {batch_idx}: {error_message}")
                        continue
                except Exception as e:
                    self.logger.error(
                        f"❌ General error in validation batch {batch_idx}: {e}")
                    continue

        if len(val_loader) > 0:
            return total_loss / len(val_loader)
        else:
            self.logger.warning(
                "Validation loader was empty. Returning 0 for validation loss.")
            return 0.0

    def predict(self, test_loader: DataLoader, model_path=None):
        if model_path:
            self.model.load_state_dict(torch.load(
                os.path.join(self.save_path, f"{model_path}_best_model.pth"), map_location=self.device))
            self.model.eval()
            self.logger.info(f"📂 Loaded model from: {model_path}")

        all_preds = []
        self.model.eval()

        with torch.no_grad():
            for x_enc, x_mark_enc, x_dec, x_mark_dec, y in test_loader:
                x_enc = x_enc.to(self.device).float()
                x_mark_enc = x_mark_enc.to(self.device).float()
                x_dec = x_dec.to(self.device).float()
                x_mark_dec = x_mark_dec.to(self.device).float()

                preds = self.model(x_enc, x_mark_enc, x_dec, x_mark_dec)
                all_preds.append(preds.cpu())

        return torch.cat(all_preds, dim=0)

    def trainAll(self):
        for symbol in self.symbol_dict.keys():
            try:
                loaders = self.get_data_loader_from_symbol(symbol)
                save_file = f"{symbol}_best_model.pth"
                self.train(loaders, save_path=save_file)
            except Exception as e:
                self.logger.error(f"❌ Failed to train {symbol}: {e}")
