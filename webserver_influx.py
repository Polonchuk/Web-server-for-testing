from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
from multiprocessing import Process, Queue

from influxdb import InfluxDBClient
from datetime import datetime
import time


def start_server(q, serverHost, serverPort):
    
    class MathServer(BaseHTTPRequestHandler):
        
        def do_GET(self):

            if q.empty() == True:
                result=1
                q.put(result)
            else:
                result = q.get()
                result+=1
                q.put(result)

            self.send_response(200)
            query_values_sum = 0
            try:
                url = urlparse(self.path)
                self.query = parse_qs(url.query)
                query_values = self.query.values()

                for val in query_values:
                    try:
                        numeric_val = float(val[0])
                        query_values_sum += numeric_val

                    except:
                        print(f'Unexpected value: {val[0]}\n')
                    
                self.send_header("Content-type", "text/csv")
                self.end_headers()
                self.wfile.write(bytes(f'Sum: {query_values_sum}', "utf-8"))

            except:
                print(f'Unparsed URL structure!\n')

        # print(self.headers)
        # print(f'Sum: {query_values_sum}')

    httpd = HTTPServer((serverHost, serverPort), MathServer)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()
    print('Math server stopped.')


def report_to_influxdb(q, dbHost, dbPort):

    # start_time = datetime.now()
    try:
        while True:
            time.sleep(60)

            if q.empty() == True:
                print('Queue is empty')
            else:
                result = q.get()
                q.put(result)
                print(f"Number of GET-requests to MathServer:{result}")

    except KeyboardInterrupt:
        pass
    print('DB report stopped.')


if __name__ == '__main__':

    serverHost = 'localhost'
    serverPort = 8008
    dbHost = 'localhost'
    dbPort = 8086

    q = Queue()

    print(f'Listening on http://{serverHost}:{serverPort}\n')

    running_server = Process(target=start_server, args=(q, serverHost, serverPort))
    running_db = Process(target=report_to_influxdb, args=(q, dbHost, dbPort))
    
    running_server.start()
    running_db.start()
    
    running_server.join()
    running_db.join()
