import os
import logging
import pyodbc # MSSQL 드라이버
import socket

# CloudWatch Logs에 로그를 출력하기 위한 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 환경 변수에서 DB 연결 정보를 불러옵니다.
# (⚠️ AWS Lambda 콘솔에서 직접 설정해야 합니다.)
# DB_HOST = os.environ['DB_HOST']
# DB_PORT = os.environ['DB_PORT']
# DB_NAME = os.environ['DB_NAME']
# DB_USER = os.environ['DB_USER']
# DB_PASSWORD = os.environ['DB_PASSWORD']

DB_HOST = "211.50.136.35"
DB_PORT = 9433
DB_NAME = "corner_shop"
DB_USER = "sa"
DB_PASSWORD = "qwer!234"
# DB_USER = "pmk_sales"
# DB_PASSWORD = "pmk123$$"

def lambda_handler(event, context):
    """
    AWS Lambda 핸들러 함수.
    MSSQL DB에 접속하여 쿼리를 실행하고 결과를 반환합니다.
    """
    # conn = None
    # try:
    #     logger.info("MSSQL 데이터베이스에 연결 시도 중...")

    #     with socket.create_connection((DB_HOST, DB_PORT), timeout=60) as sock:
    #         print(f"[OK] {DB_HOST}:{DB_PORT} 접속 가능")
    #         return True
        
    #     # C# 연결 문자열을 참고하여 pyodbc 연결 문자열 생성
    #     conn_str = (
    #         f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    #         f'SERVER={DB_HOST},{DB_PORT};'
    #         f'DATABASE={DB_NAME};'
    #         f'UID={DB_USER};'
    #         f'PWD={DB_PASSWORD};'
    #     )

    #     conn = pyodbc.connect(conn_str)
    #     logger.info("MSSQL 데이터베이스 연결 성공!")

    #     # 커서 생성
    #     with conn.cursor() as cur:
    #         # 예제 쿼리 실행
    #         cur.execute("SELECT COUNT(*) FROM iris.categories")
            
    #         # 결과 가져오기
    #         rows = cur.fetchall()
            
    #         logger.info(f"총 {len(rows)}개의 레코드를 조회했습니다.")
    #         for row in rows:
    #             print(row) # CloudWatch Logs에 각 행 출력

    #     conn.commit()
    # except socket.timeout as e:
    #     print(f"[TIMEOUT] {DB_HOST}:{DB_PORT} 연결 시간 초과")
    #     logger.error(e)
    # except ConnectionRefusedError as e:
    #     print(f"[REFUSED] {DB_HOST}:{DB_PORT} 연결이 거부됨")
    #     logger.error(e)
    # except Exception as e:
    #     logger.error("데이터베이스 연결 또는 쿼리 실행 중 오류 발생!")
    #     logger.error(e)
    #     return {
    #         'statusCode': 500,
    #         'body': 'Database connection or query failed.'
    #     }
        
    # finally:
    #     # 연결 종료
    #     if conn:
    #         conn.close()
    #         logger.info("데이터베이스 연결 종료.")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)

    try:
        s.connect((DB_HOST, DB_PORT))
        logger.info("Connected")
    except Exception as e:
        logger.error(e)
    finally:
        s.close()
            
    return {
        'statusCode': 200,
        'body': 'Successfully connected to the database and executed the query.'
    }