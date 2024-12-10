import socket
import time
import struct
from control_algorithm.adaptive_tau import ControlAlgAdaptiveTauClient
from data_reader.data_reader import get_data, get_data_train_samples
from models.get_model import get_model
from util.sampling import MinibatchSampling
from util.utils import send_msg, recv_msg
from config import SERVER_ADDR_PRIMARY, SERVER_PORT_PRIMARY, SERVER_ADDR_SECONDARY, SERVER_PORT_SECONDARY, dataset_file_path

# Configuration to determine which server to connect to
connect_to_primary = True  # Set to False for connecting to the secondary server

server_address = (SERVER_ADDR_PRIMARY, SERVER_PORT_PRIMARY) if connect_to_primary else (SERVER_ADDR_SECONDARY, SERVER_PORT_SECONDARY)

# Connect to the appropriate server
sock = socket.socket()
sock.connect(server_address)
print(f"Connected to {'primary' if connect_to_primary else 'secondary'} server at {server_address}")

print('---------------------------------------------------------------------------')

batch_size_prev = None
total_data_prev = None
sim_prev = None

try:
    while True:
        # Receive initialization message from server
        msg = recv_msg(sock, 'MSG_INIT_SERVER_TO_CLIENT')

        model_name = msg[1]
        dataset = msg[2]
        num_iterations_with_same_minibatch_for_tau_equals_one = msg[3]
        step_size = msg[4]
        batch_size = msg[5]
        total_data = msg[6]
        control_alg_server_instance = msg[7]
        indices_this_node = msg[8]
        read_all_data_for_stochastic = msg[9]
        use_min_loss = msg[10]
        sim = msg[11]

        # Initialize the model
        model = get_model(model_name)
        model2 = get_model(model_name)  

        if hasattr(model, 'create_graph'):
            model.create_graph(learning_rate=step_size)
        if hasattr(model2, 'create_graph'):
            model2.create_graph(learning_rate=step_size)

        # Load dataset
        if read_all_data_for_stochastic or batch_size >= total_data:
            if batch_size_prev != batch_size or total_data_prev != total_data or (batch_size >= total_data and sim_prev != sim):
                print('Reading all data samples used in training...')
                train_image, train_label, _, _, _ = get_data(dataset, total_data, dataset_file_path, sim_round=sim)

        batch_size_prev = batch_size
        total_data_prev = total_data
        sim_prev = sim

        if batch_size >= total_data:
            sampler = None
            train_indices = indices_this_node
        else:
            sampler = MinibatchSampling(indices_this_node, batch_size, sim)
            train_indices = None 
        last_batch_read_count = None

        data_size_local = len(indices_this_node)

        if isinstance(control_alg_server_instance, ControlAlgAdaptiveTauClient):
            control_alg = ControlAlgAdaptiveTauClient()
        else:
            control_alg = None

        w_prev_min_loss = None
        w_last_global = None
        total_iterations = 0

        # Notify server that data preparation is complete
        msg = ['MSG_DATA_PREP_FINISHED_CLIENT_TO_SERVER']
        send_msg(sock, msg)

        while True:
            print('---------------------------------------------------------------------------')

            # Receive weights and configuration from server
            msg = recv_msg(sock, 'MSG_WEIGHT_TAU_SERVER_TO_CLIENT')
            w = msg[1]
            tau_config = msg[2]
            is_last_round = msg[3]
            prev_loss_is_min = msg[4]

            if prev_loss_is_min or ((w_prev_min_loss is None) and (w_last_global is not None)):
                w_prev_min_loss = w_last_global

            if control_alg is not None:
                control_alg.init_new_round(w)

            time_local_start = time.time()  

            grad = None
            loss_last_global = None  
            loss_w_prev_min_loss = None

            tau_actual = 0

            for i in range(0, tau_config):

                if batch_size < total_data:

                    if (not isinstance(control_alg, ControlAlgAdaptiveTauClient)) or (i != 0) or (train_indices is None) \
                            or (tau_config <= 1 and
                                (last_batch_read_count is None or
                                 last_batch_read_count >= num_iterations_with_same_minibatch_for_tau_equals_one)):

                        sample_indices = sampler.get_next_batch()

                        if read_all_data_for_stochastic:
                            train_indices = sample_indices
                        else:
                            train_image, train_label = get_data_train_samples(dataset, sample_indices, dataset_file_path)
                            train_indices = range(0, min(batch_size, len(train_label)))

                        last_batch_read_count = 0

                    last_batch_read_count += 1

                grad = model.gradient(train_image, train_label, w, train_indices)

                if i == 0:
                    try:
                        loss_last_global = model.loss_from_prev_gradient_computation()
                        print('*** Loss computed from previous gradient computation')
                    except:
                        loss_last_global = model.loss(train_image, train_label, w, train_indices)
                        print('*** Loss computed from data')

                    w_last_global = w

                    if use_min_loss:
                        if (batch_size < total_data) and (w_prev_min_loss is not None):
                            loss_w_prev_min_loss = model2.loss(train_image, train_label, w_prev_min_loss, train_indices)

                w = w - step_size * grad

                tau_actual += 1
                total_iterations += 1

                if control_alg is not None:
                    is_last_local = control_alg.update_after_each_local(i, w, grad, total_iterations)

                    if is_last_local:
                        break

            time_local_end = time.time()
            time_all_local = time_local_end - time_local_start
            print('time_all_local =', time_all_local)

            if control_alg is not None:
                control_alg.update_after_all_local(model, train_image, train_label, train_indices,
                                                   w, w_last_global, loss_last_global)

            # Send updated weights, time, and other stats to the server
            msg = ['MSG_WEIGHT_TIME_SIZE_CLIENT_TO_SERVER', w, time_all_local, tau_actual, data_size_local,
                   loss_last_global, loss_w_prev_min_loss]
            send_msg(sock, msg)

            if control_alg is not None:
                control_alg.send_to_server(sock)

            if is_last_round:
                break

except (struct.error, socket.error):
    print('Server has stopped')
    pass
