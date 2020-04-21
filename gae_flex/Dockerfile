FROM gcr.io/google-appengine/python

# Create a virtualenv for dependencies. This isolates these packages from
# system-level packages.
# Use -p python3 or -p python3.7 to select python version. Default is version 2.
RUN virtualenv -p python3.7 /env

# Setting these environment variables are the same as running
# source /env/bin/activate.
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH

# Copy the application's requirements.txt and run pip to install all
# dependencies into the virtualenv.
ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Add the application source code.
ADD . /app

RUN git clone https://github.com/all-of-us/curation /app/curation
RUN sed -i "s/MarkupSafe==1.0/MarkupSafe==1.1/" /app/curation/data_steward/requirements.txt
RUN pip install -r /app/curation/data_steward/requirements.txt
RUN pip install -r /app/curation/data_steward/dev_requirements.txt
RUN pip install -r /app/curation/data_steward/deid/requirements.txt

# Run a WSGI server to serve the application. gunicorn must be declared as
# a dependency in requirements.txt.
CMD gunicorn -b :$PORT main:app