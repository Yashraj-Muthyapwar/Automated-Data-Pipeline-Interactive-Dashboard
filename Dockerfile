# Dockerfile

# This instruction sets the base image for the subsequent build stages.
# We are choosing an official Python image, specifically version 3.9,
# and the 'slim' variant which is a smaller, production-optimized image.
# This MUST be the first line in the Dockerfile.
FROM python:3.9-slim


# The WORKDIR instruction sets the working directory for any subsequent commands.
# If the directory doesn't exist, Docker will create it.
# This helps to keep the container organized and our commands clean.
# All subsequent COPY, RUN, and CMD instructions will be executed from this directory.
WORKDIR /app

# The COPY instruction copies files from the host machine into the container's filesystem.
# We copy the requirements file first to leverage Docker's layer caching.
# The dependencies will only be re-installed if this file changes.
# The '.' at the end specifies the destination is the current working directory, which is '/app'.
COPY requirements.txt .

# The RUN instruction executes commands in a new layer on top of the current image.
# Here, we are using pip to install the Python dependencies defined in requirements.txt.
# The --no-cache-dir flag is a crucial optimization that prevents pip from storing
# the download cache, which significantly reduces the final image size.
RUN pip install --no-cache-dir -r requirements.txt


# Now that dependencies are installed, copy the rest of the application's source code.
# The '.' as the source refers to the current directory on the host (the build context).
# The '.' as the destination refers to the current working directory in the container ('/app').
# The .dockerignore file ensures that only necessary files are copied.
COPY . .


# The CMD instruction provides the default command to execute when a container is started.
# We use the "exec" form (a JSON array) which is the recommended best practice.
# This will execute `python main.py` as the main process inside the container.
CMD ["python", "main.py"]
