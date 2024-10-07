import flwr as fl
import matplotlib.pyplot as plt
from utils import logger
# List to store the loss values
loss_values = []

# Function to update the plot
def update_plot(loss_values):
    plt.clf()  # Clear the current figure
    plt.plot(range(1, len(loss_values) + 1), loss_values, marker='o')
    plt.xlabel('Round')
    plt.ylabel('Loss')
    plt.title('Training Loss per Round')
    
    
    
    plt.grid(True)
    plt.savefig(f'loss_plot.png')  # Save plot as image

class CustomStrategy(fl.server.strategy.FedAvg):
    def aggregate_evaluate(self, rnd, results, failures):
        # Call the parent class method to get the aggregated loss
        aggregated_loss = super().aggregate_evaluate(rnd, results, failures)
        if aggregated_loss is not None:
            loss_values.append(aggregated_loss[0])
            update_plot(loss_values)
        
        logger.info(f'Aggregated evaluation completes with aggregated loss {aggregated_loss}')
        return aggregated_loss

# Initialize the plot
# plt.ion()
# plt.figure()

# Start the server
fl.server.start_server(
    server_address="0.0.0.0:51000",
    config=fl.server.ServerConfig(num_rounds=100),
    strategy=CustomStrategy()
)

# Keep the plot open after training is done
# plt.ioff()
# plt.show()
