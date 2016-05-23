# Dockerfile extending the generic Python image with application files for a
# single application.
FROM gcr.io/google_appengine/python-compat

# Update the APT package index
RUN apt-get update
# Install wget to download ffmpeg
RUN apt-get install -y wget
# Install following libraries for the lxml Python requirement
RUN apt-get install -y gcc libxml2-dev libxslt1-dev python-dev

# Install a more recent version of distribute (>=0.6.49) for
# the GoogleAppEngineCloudStorageClient Python requirement
RUN pip install -U distribute

# Install all Python requirements
ADD requirements.txt /app/
RUN pip install -r requirements.txt

# Download and setup ffmpeg from a static build
RUN cd /usr/local/bin && mkdir ffmpeg && cd ffmpeg \
    && wget -q http://johnvansickle.com/ffmpeg/releases/ffmpeg-release-64bit-static.tar.xz \
    && tar -xf ffmpeg-release-64bit-static.tar.xz --strip-components=1 \
    && rm ffmpeg-release-64bit-static.tar.xz \
    && ln -s /usr/local/bin/ffmpeg/ffmpeg /usr/bin/ffmpeg \
    && ln -s /usr/local/bin/ffmpeg/ffprobe /usr/bin/ffprobe

ADD . /app
