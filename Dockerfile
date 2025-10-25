
FROM python:3.12-slim
WORKDIR /app

COPY pyproject.toml poetry.lock* ./
RUN pip install --no-cache-dir poetry \
 && poetry config virtualenvs.create false
RUN poetry install --no-interaction --no-ansi --no-root
COPY . /app
CMD ["python", "main.py"]