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
                result = 1
                q.put(result)
            else:
                result = q.get()
                result += 1
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
    print(f'Math server listening on http://{serverHost}:{serverPort}\n')

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()
    print('Math server stopped.')


def report_to_influxdb(q, dbHost, dbPort):

    client = InfluxDBClient(dbHost, dbPort, 'math_server', 'math_server1', 'math_server')
    client.create_database('math_server')
    client.switch_database('math_server')
    requests_reported = 0

    try:
        while True:
            time.sleep(60)

            if q.empty() == False:
                
                requests_total = q.get()
                q.put(requests_total)
                requests_to_report = requests_total - requests_reported
            else:
                requests_total = 0
                requests_to_report = 0
            
            report = [{
                "measurement": "requests",
                "tags": {
                    "request": "GET_per_1_minute" 
                    },
                "time": datetime.now(),
                "fields": {
                    'requests_number': requests_to_report,
                }
            }]
            
            try:
                client.write_points(report)
                print(f'Report sent: Requests per time period: {requests_to_report}, requests from the start: {requests_total}')
                requests_reported = requests_reported + requests_to_report
            except:
                print(f'Report was NOT sent at: {datetime.now()}')
                
    except KeyboardInterrupt:
        pass
    print('DB report stopped.')


if __name__ == '__main__':

    serverHost = 'localhost'
    serverPort = 8008
    dbHost = 'localhost'
    dbPort = 8086

    q = Queue()

    running_server = Process(target=start_server, args=(q, serverHost, serverPort))
    running_db = Process(target=report_to_influxdb, args=(q, dbHost, dbPort))
    
    running_server.start()
    running_db.start()
    
    running_server.join()
    running_db.join()
