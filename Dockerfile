
FROM python:3.12-slim
WORKDIR /app

# Install Poetry and disable venvs so deps go to system site-packages
RUN pip install --no-cache-dir poetry \
 && poetry config virtualenvs.create false

# If you have poetry.lock, copy it too for reproducible builds
COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-interaction --no-ansi --no-root

COPY . /app
CMD ["python", "main.py"]

# TODO: udostępnij folder logs jako wolumen