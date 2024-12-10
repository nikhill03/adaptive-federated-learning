import math
import numpy as np
from numpy import linalg
from util.utils import recv_msg, send_msg, moving_average
from config import tau_max

class ControlAlgAdaptiveTauServer:
    def __init__(self, is_adapt_local, dim_w, client_sock_all, n_nodes, control_param_phi, moving_average_holding_param):
        self.is_adapt_local = is_adapt_local
        self.dim_w = dim_w
        self.client_sock_all = client_sock_all
        self.n_nodes = n_nodes
        self.control_param_phi = control_param_phi
        self.moving_average_holding_param = moving_average_holding_param
        self.beta_adapt_mvaverage = None
        self.delta_adapt_mvaverage = None
        self.rho_adapt_mvaverage = None

    def compute_new_tau(self, data_size_local_all, data_size_total, it_each_local, it_each_global, max_time, step_size, tau, use_min_loss):
        beta_adapt = 0
        delta_adapt = 0
        rho_adapt = 0
        global_grad_global_weight = np.zeros(self.dim_w)
        local_grad_global_weight_all = []
        control_param_computed = False

        for n in range(self.n_nodes):
            msg = recv_msg(self.client_sock_all[n], 'MSG_CONTROL_PARAM_COMPUTED_CLIENT_TO_SERVER')
            control_param_computed_this_client = msg[1]
            control_param_computed = control_param_computed or control_param_computed_this_client

            if control_param_computed_this_client:
                msg = recv_msg(self.client_sock_all[n], 'MSG_BETA_RHO_GRAD_CLIENT_TO_SERVER')
                beta_adapt_local = msg[1]
                rho_adapt_local = msg[2]
                local_grad_global_weight = msg[3]
                local_grad_global_weight_all.append(local_grad_global_weight)
                beta_adapt += data_size_local_all[n] * beta_adapt_local
                rho_adapt += data_size_local_all[n] * rho_adapt_local
                global_grad_global_weight += data_size_local_all[n] * local_grad_global_weight

        global_grad_global_weight /= data_size_total

        if control_param_computed and it_each_local is not None and it_each_global is not None:
            beta_adapt /= data_size_total
            rho_adapt /= data_size_total

            for i in range(self.n_nodes):
                delta_adapt += data_size_local_all[i] * linalg.norm(local_grad_global_weight_all[i] - global_grad_global_weight)
            delta_adapt /= data_size_total

            self.beta_adapt_mvaverage = moving_average(self.beta_adapt_mvaverage, beta_adapt, self.moving_average_holding_param)
            self.delta_adapt_mvaverage = moving_average(self.delta_adapt_mvaverage, delta_adapt, self.moving_average_holding_param)
            self.rho_adapt_mvaverage = moving_average(self.rho_adapt_mvaverage, rho_adapt, self.moving_average_holding_param)

            if self.is_adapt_local:
                min_tau_new_tmp = 1
                min_val = float('inf')

                for tau_new_tmp in range(1, tau * 10 + 1):
                    h_tau_tmp = max(0.0, (self.delta_adapt_mvaverage / self.beta_adapt_mvaverage) * (
                        np.power(step_size * self.beta_adapt_mvaverage + 1, tau_new_tmp) - 1) - self.delta_adapt_mvaverage * step_size * tau_new_tmp)

                    if use_min_loss:
                        tmp_adjusted_T = (max_time - it_each_local - it_each_global) * tau_new_tmp / (it_each_local * tau_new_tmp + it_each_global)
                    else:
                        tmp_adjusted_T = max_time * tau_new_tmp / (it_each_local * tau_new_tmp + it_each_global)

                    tmp_gap = (1 + math.sqrt(max(0.0, 1 + 4 * math.pow(tmp_adjusted_T, 2.0)
                                                * self.control_param_phi * self.rho_adapt_mvaverage
                                                * step_size * h_tau_tmp / tau_new_tmp))) / (
                        2 * tmp_adjusted_T * step_size * self.control_param_phi) + self.rho_adapt_mvaverage * h_tau_tmp

                    if tmp_gap < min_val:
                        min_val = tmp_gap
                        min_tau_new_tmp = tau_new_tmp

                tau_new = min_tau_new_tmp
            else:
                tau_new = tau
        else:
            tau_new = tau

        return min(tau_new, tau_max)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['client_sock_all']
        return state

class ControlAlgAdaptiveTauClient:
    def __init__(self):
        self.w_last_local_last_round = None
        self.grad_last_local_last_round = None
        self.loss_last_local_last_round = None

    def init_new_round(self, w):
        self.control_param_computed = False
        self.beta_adapt = None
        self.rho_adapt = None
        self.grad_last_global = None

    def update_after_each_local(self, iteration_index, w, grad, total_iterations):
        if iteration_index == 0:
            self.grad_last_global = grad
        return False

    def update_after_all_local(self, model, train_image, train_label, train_indices, w, w_last_global, loss_last_global):
        if self.w_last_local_last_round is not None and self.grad_last_local_last_round is not None and self.loss_last_local_last_round is not None:
            c = self.grad_last_local_last_round - self.grad_last_global
            tmp_norm = linalg.norm(self.w_last_local_last_round - w_last_global)
            if tmp_norm > 1e-10:
                self.beta_adapt = linalg.norm(c) / tmp_norm
            else:
                self.beta_adapt = 0

            if tmp_norm > 1e-10:
                self.rho_adapt = linalg.norm(self.loss_last_local_last_round - loss_last_global) / tmp_norm
            else:
                self.rho_adapt = 0

            if self.beta_adapt < 1e-5 or np.isnan(self.beta_adapt):
                self.beta_adapt = 1e-5

            if np.isnan(self.rho_adapt):
                self.rho_adapt = 0

            self.control_param_computed = True

        self.grad_last_local_last_round = model.gradient(train_image, train_label, w, train_indices)

        try:
            self.loss_last_local_last_round = model.loss_from_prev_gradient_computation()
        except:
            self.loss_last_local_last_round = model.loss(train_image, train_label, w, train_indices)

        self.w_last_local_last_round = w

    def send_to_server(self, sock):
        msg = ['MSG_CONTROL_PARAM_COMPUTED_CLIENT_TO_SERVER', self.control_param_computed]
        send_msg(sock, msg)

        if self.control_param_computed:
            msg = ['MSG_BETA_RHO_GRAD_CLIENT_TO_SERVER', self.beta_adapt, self.rho_adapt, self.grad_last_global]
            send_msg(sock, msg)
