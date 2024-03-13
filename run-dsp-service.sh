#!/bin/bash
cd /home/dspapp/dsp-service
. /home/dspapp/dsp-service/.venv/bin/activate
source env.sh
gunicorn --bind 0.0.0.0:9111 'wsgi:create_app()'
