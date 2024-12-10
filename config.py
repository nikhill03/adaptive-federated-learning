from util.time_generation import TimeGeneration
import os

# Server configurations for the decentralized setup
SERVER_ADDR_PRIMARY = 'localhost'  # Address of the primary server
SERVER_PORT_PRIMARY = 51000        # Port of the primary server

SERVER_ADDR_SECONDARY = 'localhost'  # Address of the secondary server
SERVER_PORT_SECONDARY = 52000        # Port of the secondary server

dataset_file_path = os.path.join(os.path.dirname(__file__), 'datasets')
results_file_path = os.path.join(os.path.dirname(__file__), 'results')
single_run_results_file_path = results_file_path + '/SingleRun.csv'
multi_run_results_file_path = results_file_path + '/MultipleRuns.csv'

# Model, dataset, and control parameter configurations for MNIST with SVM
dataset = 'MNIST_ORIG_EVEN_ODD'  # Use for SVM model
model_name = 'ModelSVMSmooth'
control_param_phi = 0.025  # Good for MNIST with smooth SVM

# Model, dataset, and control parameter configurations for MNIST with CNN
# dataset = 'MNIST_ORIG_ALL_LABELS'  # Use for CNN model
# model_name = 'ModelCNNMnist'
# control_param_phi = 0.00005  # Good for CNN

n_nodes = 2  # Specifies the total number of clients

moving_average_holding_param = 0.0  # Moving average coefficient to smooth the estimation of beta, delta, and rho

step_size = 0.01

# Batch and data configurations
# Setting batch_size equal to total_data makes the system use deterministic gradient descent;
# Setting batch_size < total_data makes the system use stochastic gradient descent.
batch_size = 100  # Value for stochastic gradient descent
total_data = 60000  # Value for stochastic gradient descent

# Single vs. multiple simulation runs
single_run = False

# Estimation of beta and delta
estimate_beta_delta_in_all_runs = False

# Loss selection mode
use_min_loss = True

# Minibatch reuse configuration
num_iterations_with_same_minibatch_for_tau_equals_one = 3

# Data reading mode for stochastic gradient descent
read_all_data_for_stochastic = True

MAX_CASE = 4  # Specifies the maximum number of cases
tau_max = 100  # Specifies the maximum value of tau

# Tau setup for adaptive or fixed values
if not single_run:
    tau_setup_all = [-1, 1, 2, 3, 5, 7, 10, 20, 30, 50, 70, 100]
    sim_runs = range(0, 2)  # Simulation seeds for each round
    case_range = range(0, MAX_CASE)
else:
    case_range = [0]  # Single case for single run
    tau_setup_all = [-1]  # Single tau value for single run
    sim_runs = [0]  # Single random seed for single run

# Time budget
max_time = 15  # Total time budget in seconds

# Time generation configuration
multiply_global = 1.0
multiply_local = 1.0
time_gen = TimeGeneration(multiply_local * 0.013015156, multiply_local * 0.006946299, 1e-10,
                          multiply_global * 0.131604348, multiply_global * 0.053873234, 1e-10)

