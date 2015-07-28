#!/usr/bin/env bash

# Install system requirements
sudo apt-get update
sudo apt-get install -y python3-pip libxml2-dev libxslt-dev zlib1g-dev mongodb git redis-server rabbitmq-server

# Fix pip
pip3 install --upgrade pip

# Install app requirements
pip install --upgrade -r /vagrant/requirements.txt

# Fix the default python instance
update-alternatives --install /usr/bin/python python /usr/bin/python2.7 1
update-alternatives --install /usr/bin/python python /usr/bin/python3.4 2

# Put in a default local_settings.py (if one doesn't exist)
if [ ! -f /vagrant/tapiriik/local_settings.py ]; then
    cp /vagrant/tapiriik/local_settings.py.example /vagrant/tapiriik/local_settings.py
    # Generate credential storage keys
    python /vagrant/credentialstore_keygen.py >> /vagrant/tapiriik/local_settings.py
fi

