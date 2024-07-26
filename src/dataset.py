from configparser import ConfigParser
import os
from utils import logger

class Dataset(object):

    def __init__(self):
        self.data = []

    def augment_data(self, new_point):
        self.data.append(new_point)
