import cv2
import glob
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import sys
from consume_green import *


# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)


# 轉換msgKey或msgValue成為utf-8的字串
def try_decode_utf8(data):
    if data:
        print("成功")
        return data.decode('utf-8')
    else:
        return None


# 指定要從哪個partition, offset開始讀資料
# def my_assign(consumer_instance, partitions):
#     for p in partitions:
#         p.offset = 0
#     print('assign', partitions)
#     consumer_instance.assign(partitions)

def item_consume_red():
    props = {
        'bootstrap.servers': '10.1.1.133:9092',  # Kafka集群在那裡? (置換成要連接的Kafka集群)
        'group.id': 'STUDENT_ID',  # ConsumerGroup的名稱 (置換成你/妳的學員ID)
        'auto.offset.reset': 'latest',  # Offset從最前面開始
        'enable.auto.commit': True,  # auto commit
        'session.timeout.ms': 6000,  # consumer超過6000ms沒有與kafka連線，會被認為掛掉了
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    # topicName1 = "items"
    topicName2 = "items2"
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    # consumer.subscribe([topicName1])
    consumer.subscribe([topicName2])
    # 步驟5. 持續的拉取Kafka有進來的訊息 # on_assign=my_assign
    count = 0
    try:
        while True:
            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% {} [{}] reached end at offset {} - {}\n'.format(record.topic(),
                                                                                             record.partition(),
                                                                                             record.offset()))

                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    topic = record.topic()
                    partition = record.partition()
                    offset = record.offset()
                    timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())
                    # 秀出metadata與msgKey & msgValue訊息
                    count += 1
                    print('{}-{}-{} : ({} , {})'.format(topic, partition, offset, msgKey, msgValue))
                    consumer.close()
                    # print("consumer")
    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(str(e))

    finally:
        # 步驟6.關掉Consumer實例的連線
        consumer.close()
        # print('wrong')



def camera_capture():
    model = cv2.face.LBPHFaceRecognizer_create()
    face_cascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')

    cap = cv2.VideoCapture(1)

    while (cap.isOpened()):
        ret, img = cap.read()
        if ret == True:
            img = cv2.resize(img, (640, 480))
            img = cv2.flip(img, 1)

            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            faces = face_cascade.detectMultiScale(gray, 1.1, 3)

            for (x, y, w, h) in faces:
                img = cv2.rectangle(img, (x, y), (x + w, y + h), (0, 255, 0), 3)
                face_img = cv2.resize(gray[y: y + h, x: x + w], (400, 400))
                confidence_num = []
                path_list = glob.glob('model/*.data')
                for path in path_list:
                    model.read(path)
                    params = model.predict(face_img)
                    confidence_num.append(params[1])
                Who = path_list[confidence_num.index(min(confidence_num))]

            cap.release()
            break
    cap.release()
    return Who.split('\\')[1].split('.')[0]


def member_produce():
    props = {
        # Kafka集群在那裡?
        'bootstrap.servers': '10.1.1.133:9092',  # <-- 置換成要連接的Kafka集群
        # 'bootstrap.servers': 'localhost:9092',  # <-- 置換成要連接的Kafka集群

        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }
    producer = Producer(props)
    # 步驟3. 指定想要發佈訊息的topic名稱
    topicName = 'transaction'
    # topicName = 'test3'
    msgCounter = 0
    member = camera_capture()
    # aa="bbb"
    try:
        # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])
        producer.produce(topicName, member, "Name")
        producer.flush()
        msgCounter += 1
        print('Send ' + str(msgCounter) + ' messages to Kafka')
        print('The member is ' + member)
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                         .format(len(producer)))
    except Exception as e:
        print(e)
    # 步驟5. 確認所在Buffer的訊息都己經送出去給Kafka了
    producer.flush()


# 主程式進入點
if __name__ == '__main__':
    while True:
        try:
            item_consume_red()
            print("black tea consume success! Next step")

            # item_consume_green()
            # print("green tea consume success! Next step")

            camera_capture()
            print("Member identify success！")

            member_produce()
            print("Member produce is done！")
            print('=' * 30)
        except:
            print("Failure")

        continue


