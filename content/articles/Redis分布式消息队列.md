Title: Redis分布式消息队列
Date: 2025-11-28 16:00
Category: 入门
Tags: Redis, Python

#### 一、消息队列基础概述

##### 1.1 定义

**消息队列（Message Queue）** 是一种应用程序间的异步通信机制，

核心采用 生产者-消费者模型(Producer-Consumer Model)

生产者（Producer）将任务消息发布到队列中；

一个或多个消费者（Consumer/Worker）从队列中获取并处理这些任务。

##### 1.2 核心架构


| 组件名称                      | 核心作用                                  | 常用实现方案                    |
| ----------------------------- | ----------------------------------------- | ------------------------------- |
| **Producer（生产者）**        | 创建任务消息，并将其推入 Redis 队列。     | 业务应用中的某个函数或服务。    |
| **Redis（消息中间件）**       | 存储任务消息队列。                        | Redis 服务器。                  |
| **Consumer（消费者/Worker）** | 从 Redis 队列中获取任务，并执行具体逻辑。 | 一个或多个独立的后台脚本/进程。 |

#### 二、核心Redis命令

Redis 提供了多种数据结构来实现消息队列，最常用的是 **List（列表）**。

##### 2.1 使用 List 实现简单队列

List 是一个双向链表，非常适合模拟队列的“先进先出”（FIFO）特性。

双向链表保存了头尾节点，所以在列表头尾两边插取元素都是非常快。


| 命令        | 作用                                                     | 说明                                                                                                            |
| ----------- | -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `LPUSH`     | `LPUSH queue_name value`，将一个或多个值插入到列表头部。 | 生产者用此命令发布任务。                                                                                        |
| `RPOP`      | `RPOP queue_name`，移除并获取列表的最后一个元素。        | 消费者用此命令获取任务，但如果队列为空，会立即返回`nil`，造成 CPU 空转。                                        |
| **`BRPOP`** | `BRPOP queue_name timeout`，`RPOP` 的阻塞版本。          | **推荐使用**。如果队列为空，它会阻塞连接，直到有新元素或超时。这极大地降低了 CPU 消耗，是实现高效消费者的关键。 |

**什么是CPU空转？**

> CPU 空转指的是 CPU 在没有有效任务可处理时，仍被频繁调用执行无用操作，导致计算资源被浪费的状态。
>
> **关键原因:**
>
> 1. 消费者会循环执行 RPOP 命令，队列为空时返回 nil，但循环不会停止。
> 2. 每次循环都会触发 “调用命令→接收 nil→再次调用” 的无效流程，CPU 需持续处理这些空操作。
> 3. 这些操作不产生任何业务价值，却占用 CPU 算力，导致 CPU 使用率虚高。

![image.png](images/Producer-Consumer.png)

#### 三、代码实现

下面我们通过 Python 代码演示如何构建一个完整的生产者-消费者系统。

##### 3.1 环境安装

```python
# 安装 Redis 的 Python 客户端
pip install redis
```

##### 3.2 项目目录结构

```powershell
redis_queue_demo
├── producer.py      # 生产者，负责发送任务
└── worker.py        # 消费者/Worker，负责处理任务
```

##### 3.3 生产者代码（producer.py）

生产者将一个包含任务信息的字典序列化为 JSON 字符串，然后使用 `LPUSH` 推入 Redis 队列。

```python
import redis
import json
import time

# 连接到 Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def send_email_task(email, subject, content):
    """
    Creates a task to send an email by pushing it to a Redis list.
    """
    task_data = {
        'type': 'send_email',
        'email': email,
        'subject': subject,
        'content': content,
        'timestamp': time.time()
    }
  
    task_json = json.dumps(task_data)
  
    # Use LPUSH to add the task to the left end of the 'task_queue' list
    redis_client.lpush('task_queue', task_json)
    print(f"Task added to queue: send email to {email}")

if __name__ == '__main__':
    # Send 5 email tasks as an example
    for i in range(5):
        send_email_task(
            email=f'user{i}@example.com',
            subject='Test Email',
            content=f'Hi user {i}! This is a test email from the Redis message queue.'
        )
        time.sleep(1)
```

##### 3.4 消费者代码（worker.py）

消费者使用 `BRPOP` 阻塞式地等待任务，获取到任务后反序列化并执行。

```python
import redis
import json
import time

# 连接到 Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def process_tasks():
    """
    无限循环，持续从队列中获取并处理任务
    """
    print("Worker 已启动，等待任务...")
    while True:
        try:
            # BRPOP 阻塞式地从 'task_queue' 队列尾部获取任务
            # timeout=0 表示永久阻塞，直到有任务为止
            # 返回的是一个元组 (queue_name, task_json)
            _, task_json = redis_client.brpop('task_queue', timeout=0)
  
            # 反序列化任务数据
            task_data = json.loads(task_json)
  
            print(f"\nReceived task: {task_data['type']} (From {task_data['email']})")
  
            # --- 模拟耗时的任务处理 ---
            print("Sending email...")
            time.sleep(2) # 模拟 I/O 操作
            print(f"Email sent successfully: Subject '{task_data['subject']}'")
            # --- 任务处理结束 ---

        except Exception as e:
            print(f"Error processing task: {e}")

if __name__ == '__main__':
    process_tasks()
```

##### 3.5 运行与演示

1. **启动消费者**：

在一个终端中运行 `worker.py`。

**python** **worker.py**

你会看到 "Worker started, waiting for tasks..." 的提示。

```powershell
Worker started, waiting for tasks...
```

2. **启动生产者**：

在另一个终端中运行 `producer.py`。

**python** **producer.py**

你会看到 producer 输出了 5 条 "Task added to queue" 的日志。

```powershell
Task added to queue: send email to user0@example.com
Task added to queue: send email to user1@example.com
Task added to queue: send email to user2@example.com
Task added to queue: send email to user3@example.com
Task added to queue: send email to user4@example.com
```

任务会储存到 Redis 的 "task_queue" 列表中：

![image.png](images/task_queue.png)

3. **观察消费者**：

回到第一个终端，你会看到 Worker 正在逐个接收并处理这 5 个任务，每个任务处理耗时 2 秒。

```powershell
Received task: send_email (From user0@example.com)
Sending email...
Email sent successfully: Subject 'Test Email'

Received task: send_email (From user1@example.com)
Sending email...
Email sent successfully: Subject 'Test Email'

Received task: send_email (From user2@example.com)
Sending email...
Email sent successfully: Subject 'Test Email'

Received task: send_email (From user3@example.com)
Sending email...
Email sent successfully: Subject 'Test Email'

Received task: send_email (From user4@example.com)
Sending email...
Email sent successfully: Subject 'Test Email'
```

Redis 中 "task_queue" 列表的任务将逐个被消费：

![image.png](images/task_queue2.png)

当任务被消费完，Redis会自动删除 "task_queue" 列表。

> Redis 是一个键值（Key-Value）数据库。为了保持高效的内存管理，当一个键所关联的值（比如一个 List、Set 或 Hash）变为空时，**Redis 会自动将这个键从数据库中删除**。
