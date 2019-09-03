FROM ubuntu:18.04

RUN apt-get update  && \ 
	apt-get install -y python3  python3-pip 

COPY . /usr/local/bin/ 

COPY . /home/

RUN pip3 install -r /home/requirements.txt

RUN chmod  -R 777 /home/

CMD ["python3", "/home/server.py"]

