import flwr as fl

# List to store the loss values
loss_values = []

# Function to update the plot
def update_plot(loss_values):
    with open('/tmp/loss.txt', 'w') as file:
        file.write(','.join(f"{number:.2f}" for number in loss_values))
        

class CustomStrategy(fl.server.strategy.FedAvg):
    def aggregate_evaluate(self, rnd, results, failures):
        # Call the parent class method to get the aggregated loss
        aggregated_loss = super().aggregate_evaluate(rnd, results, failures)
        if aggregated_loss is not None:
            loss_values.append(aggregated_loss[0])
            update_plot(loss_values)
        return aggregated_loss
