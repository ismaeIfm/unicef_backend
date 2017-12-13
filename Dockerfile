FROM revolutionsystems/python:3.6.3-wee-optimized-lto
COPY requirements.txt /app/
WORKDIR /app
RUN pip install -r requirements.txt
COPY unicef_backend /app
ENTRYPOINT ["python3"]
CMD ["manage.py", "runserver",  "--host", "0.0.0.0", "--port", "5000", "--debug"]
