FROM amancevice/pandas:1.0.0
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY scripts/ /scripts/
CMD tail -f /dev/null