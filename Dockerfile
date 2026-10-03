# Image légère avec Python — curl_cffi fournit des wheels précompilées,
# pas besoin d'outils de compilation supplémentaires.
FROM python:3.11-slim

WORKDIR /app

# Dépendances d'abord (profite du cache Docker si le code change mais pas elles)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code du bot
COPY vinted_bot.py .
COPY dashboard.html .

# Railway fournit PORT à l'exécution ; non utilisé au build, juste indicatif
EXPOSE 8765

CMD ["python", "vinted_bot.py"]
