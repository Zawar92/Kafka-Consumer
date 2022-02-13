FROM python:3

WORKDIR /usr/local/bin

COPY requirements.txt .

RUN pip install -r requirements.txt

RUN echo 'Getting ready to run the consumer'

COPY main.py .
COPY consumerlive.py .

CMD [ "python", "main.py" , "consumerlive.py" ]