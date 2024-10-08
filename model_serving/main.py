from flask import Flask, request, send_file, jsonify
import json
import joblib
import numpy as np
import pandas as pd
from keras.models import load_model
from utils import logger
app = Flask(__name__)


@app.route('/inference', methods=['POST'])
def inference():
    scaler = joblib.load('scaler.save')
    model = load_model('federated_model_0.h5')

    # Load new data (e.g., service A or any new dataset)
    # Replace 'ping_ue_metrics.csv' with your actual file for prediction
    #new_data = pd.read_csv('ping_ue_metrics.csv')
    new_data = pd.DataFrame(request.json)

    # Extract the 'dl_brate' feature for prediction
    window = new_data['dl_brate'].values

    # Calculate descriptive statistics for the window
    mean = np.mean(window)
    std = np.std(window)
    min_val = np.min(window)
    max_val = np.max(window)

    # Create a feature array for the window
    window_features = np.array([[mean, std, min_val, max_val]])

    # Normalize the features using the loaded scaler
    window_features_scaled = scaler.transform(window_features)

    # Predict the service type using the loaded MLP model
    prediction = model.predict(window_features_scaled)

    # Convert the prediction probability to a binary class (0 or 1)
    predicted_class = (prediction > 0.5).astype(int)

    # Output the prediction
    service_type = "Video" if predicted_class == 0 else "Ping"
    print(f"The predicted service type for the random 100-record window is: {service_type}")
    
    response = app.response_class(
        response=json.dumps({'result': service_type}),
        status=200,
        mimetype='application/json'
    )
    return response

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)
