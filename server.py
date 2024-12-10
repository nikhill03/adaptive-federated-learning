import socket
import threading
import numpy as np
import time  
from control_algorithm.adaptive_tau import ControlAlgAdaptiveTauServer
from data_reader.data_reader import get_data
from models.get_model import get_model
from statistic.collect_stat import CollectStatistics
from util.utils import send_msg, recv_msg, get_indices_each_node_case
from config import *

is_primary_server = True  
secondary_server_address = (SERVER_ADDR_SECONDARY, SERVER_PORT_SECONDARY)
heartbeat_interval = 2  # Seconds
heartbeat_timeout = 5  # Seconds

model = get_model(model_name)
if hasattr(model, 'create_graph'):
    model.create_graph(learning_rate=step_size)

if batch_size < total_data:
    train_image, train_label, test_image, test_label, train_label_orig = get_data(
        dataset, total_data, dataset_file_path
    )
    indices_each_node_case = get_indices_each_node_case(n_nodes, MAX_CASE, train_label_orig)

# Set up listening socket
listening_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listening_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_address = (SERVER_ADDR_PRIMARY, SERVER_PORT_PRIMARY) if is_primary_server else secondary_server_address
listening_sock.bind(server_address)

client_sock_all = []
secondary_sock = None

if is_primary_server:
    secondary_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        secondary_sock.connect(secondary_server_address)
        print("Connected to secondary server.")
    except Exception as e:
        print("Secondary server not reachable:", e)

def handle_client_connections():
    """Accept connections from clients."""
    while len(client_sock_all) < n_nodes:
        listening_sock.listen(5)
        print("Waiting for incoming connections...")
        client_sock, (ip, port) = listening_sock.accept()
        print('Got connection from ', (ip, port))
        client_sock_all.append(client_sock)

threading.Thread(target=handle_client_connections).start()

if single_run:
    stat = CollectStatistics(results_file_name=single_run_results_file_path, is_single_run=True)
else:
    stat = CollectStatistics(results_file_name=multi_run_results_file_path, is_single_run=False)

# Heartbeat Thread (for primary)
def send_heartbeat():
    """Periodically send heartbeat to the secondary server."""
    global is_primary_server  # Declare global variable at the start
    while True:
        if is_primary_server and secondary_sock:
            try:
                send_msg(secondary_sock, ["HEARTBEAT"])
            except Exception as e:
                print("Failed to send heartbeat:", e)
                is_primary_server = False  # Failover to secondary
                break
        time.sleep(heartbeat_interval)

if is_primary_server:
    threading.Thread(target=send_heartbeat, daemon=True).start()

# Main processing loop
while len(client_sock_all) < n_nodes:
    time.sleep(1)

n_nodes_primary = n_nodes // 2
primary_clients = client_sock_all[:n_nodes_primary]
secondary_clients = client_sock_all[n_nodes_primary:]

for sim in sim_runs:
    for case in case_range:
        for tau_setup in tau_setup_all:
            stat.init_stat_new_global_round()

            dim_w = model.get_weight_dimension(train_image, train_label)
            w_global = model.get_init_weight(dim_w, rand_seed=sim)

            if is_primary_server and secondary_sock:
                send_msg(secondary_sock, ['MSG_INIT_GLOBAL', w_global])
            elif not is_primary_server:
                msg = recv_msg(listening_sock)
                w_global = msg[1]

            while True:
                try:
                    # Communicate with clients
                    for client_sock in (primary_clients if is_primary_server else secondary_clients):
                        msg = ['MSG_WEIGHT_TAU_SERVER_TO_CLIENT', w_global, tau_setup]
                        send_msg(client_sock, msg)

                    # Collect updates from clients
                    w_global_local = np.zeros(dim_w)
                    total_data_size = 0

                    for client_sock in (primary_clients if is_primary_server else secondary_clients):
                        msg = recv_msg(client_sock)
                        w_local = msg[1]
                        data_size_local = msg[4]
                        w_global_local += w_local * data_size_local
                        total_data_size += data_size_local

                    w_global_local /= total_data_size

                    if is_primary_server and secondary_sock:
                        send_msg(secondary_sock, ['MSG_WEIGHT_UPDATE', w_global_local])
                        msg = recv_msg(secondary_sock)
                        w_global_secondary = msg[1]
                        w_global = (w_global_local + w_global_secondary) / 2
                    elif not is_primary_server:
                        msg = recv_msg(listening_sock)
                        w_global_primary = msg[1]
                        send_msg(listening_sock, ['MSG_WEIGHT_UPDATE', w_global_local])
                        w_global = (w_global_local + w_global_primary) / 2

                    if np.linalg.norm(w_global - w_global_local) < 1e-6:
                        break

                except Exception as e:
                    print("Error encountered:", e)
                    if is_primary_server:
                        send_msg(secondary_sock, ["FAILOVER"])
                    break

            stat.collect_stat_end_global_round(
                sim, case, tau_setup, max_time, model, train_image, train_label,
                test_image, test_label, w_global
            )
