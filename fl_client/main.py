import argparse, os
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.optimizers import Adam
import mlflow
import flwr as fl
import cords_semantics.tags as cords_tags
from utils import logger, UE_METRICS_FILE, PING_UE_METRICS_FILE
import joblib

mlflow.set_experiment('FL Training - Client')

# Function to load dataset and prepare data for sliding window
def load_csv_service_dataset(window_size: int=300):
    # Load and concatenate both service datasets
    service_a = pd.read_csv(f'{os.getenv("DATA_PATH")}/{UE_METRICS_FILE}', delimiter=";")
    service_b = pd.read_csv(f'{os.getenv("DATA_PATH")}/{PING_UE_METRICS_FILE}')

    # Assign service type labels
    service_a['service_type'] = 0
    service_b['service_type'] = 1

    # Combine the datasets
    data = pd.concat([service_a, service_b])

    dl_brate = data['dl_brate'].values
    ul_brate = data['ul_brate'].values

    # Prepare sliding window features and labels
    features = []
    labels = []

    for i in range(0, len(dl_brate) - window_size + 1, window_size):
        window_dl = dl_brate[i:i + window_size]
        window_ul = ul_brate[i:i + window_size]
        window_label = data['service_type'][i:i + window_size]

        # Check if the window has consistent labels
        if len(window_label.unique()) == 1:
            mean_dl = np.mean(window_dl)
            std_dl = np.std(window_dl)
            min_val_dl = np.min(window_dl)
            max_val_dl = np.max(window_dl)

            # Append features and corresponding label
            features.append([mean_dl, std_dl, min_val_dl, max_val_dl])
            labels.append(data['service_type'].iloc[i + window_size - 1])  

    # Convert to DataFrame for features and array for labels
    X = pd.DataFrame(features, columns=['mean_dl', 'std_dl', 'min_val_dl', 'max_val_dl'])
    y = np.array(labels)

    # Scale the features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Save the scaler for later use during prediction
    joblib.dump(scaler, 'scaler.save')
    logger.debug('Scaler is saved as scaler.save')

    # Split into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

    logger.info(f'Dataset has been loaded with window_size {window_size}')

    return X_train, X_test, y_train, y_test

# Create the Keras MLP model
def create_model(input_shape: int):
    model = Sequential()
    model.add(Dense(128, input_dim=input_shape, activation='relu'))
    model.add(Dense(128, activation='relu'))
    model.add(Dense(64, activation='relu'))
    model.add(Dense(1, activation='sigmoid'))

    # Compile the model
    model.compile(optimizer=Adam(learning_rate=0.0001), loss='binary_crossentropy', metrics=['accuracy'])
    
    logger.info('Model has been created')

    return model

# Flower Client for Federated Learning
class FlowerClient(fl.client.NumPyClient):
    def __init__(self, model, X_train, X_test, y_train, y_test, client_ID, current_round=0):
        self.model = model
        self.X_train = X_train
        self.X_test = X_test
        self.y_train = y_train
        self.y_test = y_test
        self.client_ID = client_ID
        self.current_round = current_round
        logger.info(f'Flower client with id {client_ID} has been initialized.')

    def get_parameters(self, config):
        return self.model.get_weights()

    def set_parameters(self, parameters):
        self.model.set_weights(parameters)

    def fit(self, parameters, config):
        self.set_parameters(parameters)
        run_name = datetime.now().strftime("%Y%m%d_%H%M%S")
        mlflow_run = mlflow.start_run(run_name=f'{run_name}_Round_{self.current_round}_Client_{self.client_ID}')

        self.model.fit(self.X_train, self.y_train, epochs=1, batch_size=16, validation_split=0.2)
        
        # Save the trained model
        self.model.save(f'federated_model_{self.client_ID}.h5')
        mlflow.set_tag(cords_tags.CORDS_RUN, mlflow_run.info.run_id)
        mlflow.tensorflow.log_model(self.model, artifact_path=f"Round_{self.current_round}_Client_{self.client_ID}")
        logger.info(f'Model is saved as federated_model_{self.client_ID}.h5')

        return self.get_parameters(config={}), len(self.X_train), {}

    def evaluate(self, parameters, config):
        self.set_parameters(parameters)
        loss, accuracy = self.model.evaluate(self.X_test, self.y_test, verbose=0)
        mlflow.log_metric('accuracy', float(accuracy))
        mlflow.log_metric('loss', float(loss))
        mlflow.end_run()

        logger.info(f'Evaluation complets with accuracy {float(accuracy)}')

        return float(loss), len(self.X_test), {"accuracy": float(accuracy)}

    def predict(self, data):
        # Predict using the loaded model
        prediction = self.model.predict(data)

        logger.info(f'Prediction result: {prediction}')

        return prediction

def preprocess_data(filepath):
    # Load the data
    data = pd.read_csv(f'{os.getenv("DATA_PATH")}/{PING_UE_METRICS_FILE}')
    data=data.tail(400)
    dl_brate= data['dl_brate'].values 
    ul_brate=data['ul_brate'].values
    mean_dl = np.mean(dl_brate)
    mean_ul = np.mean(ul_brate)
    std_dl = np.std(dl_brate)
    std_ul = np.std(ul_brate)
    min_val_dl = np.min(dl_brate)
    min_val_ul = np.min(ul_brate)
    max_val_dl = np.max(dl_brate)
    max_val_ul = np.max(ul_brate)
    features=[]
       # Append to features list
    features.append([mean_dl, std_dl, min_val_dl, max_val_dl])
    
    # Convert to DataFrame and normalize
    X = pd.DataFrame(features, columns=['mean_dl', 'std_dl', 'min_val_dl', 'max_val_dl'])
    return X


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flower Client")
    parser.add_argument("-cid", "--client-id", choices=[0, 1], type=int, help="Client ID (use 0 or 1 for partitioning)", required=True)
    parser.add_argument("-sa", "--server-address", type=str, help="Server address, i.e. 1.1.1.1:8080", required=True)
    args = parser.parse_args()

    # Load dataset
    X_train, X_test, y_train, y_test = load_csv_service_dataset(window_size=300)

    # Create the Keras model
    model = create_model(input_shape=X_train.shape[1])

    # Create the Flower client
    client = FlowerClient(model, X_train, X_test, y_train, y_test, args.client_id)

    # Start federated learning client
    fl.client.start_numpy_client(server_address=args.server_address, client=client)