Title: Redis 分布式锁：解决 Celery 任务并发冲突
Date: 2025-11-28 16:00
Category: 入门
Tags: Redis, Python

#### 一、应用背景：为什么 Celery 需要 Redis 分布式锁？

Celery 是一个强大的分布式任务队列，它允许您将耗时的操作（如发送邮件、处理文件、生成报告）放到后台由独立的 Worker 进程来执行。

为了提高处理能力，生产环境通常会运行多个 Worker 进程，甚至将它们部署在多台不同的服务器上。

当两个或更多的 Worker **在同一时刻**，从任务队列中获取到**针对同一个资源**的任务时，就会发生并发冲突。

这种架构下，容易出现以下问题：

* **定时任务重复执行**：例如每分钟执行的定时任务，若前一次任务执行超时（如处理耗时超过 1 分钟），下一个周期的 Worker 会重复启动任务；
* **并发任务资源竞争**：多个 Worker 同时执行修改同一数据库记录、调用同一第三方接口的任务，导致数据错乱或重复操作；
* **跨节点任务互斥**：本地锁（如 threading.Lock）仅能限制单个 Worker 内的并发，无法跨机器生效。

而 Redis 分布式锁具备 **「跨节点、原子性、高可用」** 特性，恰好匹配 Celery 分布式部署的需求，能实现全局任务互斥。

#### 二、Redis 分布式锁核心原理

Redis 分布式锁的核心是利用 Redis 的原子操作来保证「锁的唯一性」。

其关键设计包含两个部分：**加锁和释放锁**。

##### 2.1 加锁：原子命令 `SET ... NX ... EX`

加锁操作必须是原子的，即“检查锁是否存在”和“创建锁”这两个步骤必须合并为一步完成。Redis 的 `SET` 命令提供了完美的解决方案：

```bash
SET lock_key unique_value NX EX timeout
```

* `lock_key`：锁的唯一名称，例如 `lock:generate_report:task001`。
* `unique_value`：一个随机生成的唯一值（如 UUID），用于标识锁的持有者。这能防止一个客户端错误地释放了另一个客户端持有的锁。
* `NX` (Not exists)：这是实现互斥的关键。它告诉 Redis，只有当 `lock_key` **不存在**时，才执行 `SET` 操作。如果键已存在，命令将失败。
* `EX timeout` (Expire)：为锁设置一个自动过期时间（单位：秒）。这是一个安全阀，可以防止因 Worker 崩溃而未能手动释放锁，导致锁永久存在（即“死锁”）。

##### 2.2 释放锁：使用 Lua 脚本保证原子性

释放锁时，同样需要保证原子性。我们必须先**验证**锁是否仍然归自己所有，然后再**删除**它。如果分两步执行，可能会在验证通过后、删除前，锁因超时而过期，并被另一个 Worker 获取，此时若再执行删除，就会误删他人的锁。

最安全的做法是使用 Lua 脚本，因为 Redis 会保证整个脚本在执行期间不会被其他命令打断。

```lua
-- 脚本逻辑：先 GET 锁的值，与客户端传入的 unique_value 比较
-- 如果值匹配，说明锁确实是自己加的，则执行 DEL 删除锁
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
```

* `KEYS[1]`：代表 `lock_key`。
* `ARGV[1]`：代表 `unique_value`。

通过这种“检查并删除”的原子操作，我们就能安全地释放锁，而不会影响到其他 Worker。

幸运的是，`redis-py` 这类成熟的客户端库已经将这些复杂的底层逻辑封装好了，我们只需调用简单的方法即可，如下文代码所示。

#### 三、代码实现：防止重复执行耗时查询

让我们构思一个常见的场景：在一个数据分析平台，用户可以点击按钮，触发一个后台任务来执行一项耗时的数据查询和报表生成。

* **功能**：一个名为 `run_complex_query_task` 的 Celery 任务，它接收一个 `query_id` 作为参数，代表一个特定的查询请求。
* **流程**：

  * 根据 `query_id` 获取查询参数。
  * 连接数据库，执行一个可能耗时数分钟的复杂 SQL 查询。
  * 处理查询结果，生成报表，并缓存结果。
* **问题**：

  * 如果用户因为网络延迟或不耐烦，**连续多次点击**了“生成报表”按钮，会导致同一个 `run_complex_query_task` 任务被多次放入队列。
  * **Worker A** 获取到第一个任务，开始执行耗时的数据库查询。
  * 几乎在同一时刻，**Worker B** 获取到第二个任务，也开始执行完全相同的查询。
* **后果**：


  * **数据库过载**：多个重量级查询同时在数据库上运行，消耗大量 CPU 和 I/O，可能拖慢整个应用。
  * **资源浪费**：服务器重复执行了相同的计算和数据处理，浪费了宝贵的计算资源。
  * **结果覆盖**：如果任务需要将结果写入缓存或文件，后完成的任务会覆盖先完成的，造成不必要的写操作。

**分布式锁**可以确保在任何时刻，只有一个 Worker 能为指定的 `query_id` 执行查询任务。

下面我们通过一个完整的 Python 脚本来演示如何解决这个问题。

##### 3.1 环境安装

```bash
pip install celery redis
```

##### 3.2 示例代码 (`tasks.py`)

```python
import os
import time
import uuid
from celery import Celery
from redis import Redis
from redis.lock import Lock
from redis.exceptions import LockError

# 1. Celery 和 Redis 客户端初始化
# 假设 Redis 运行在本地
BROKER_URL = 'redis://:000000@127.0.0.1:6379/0'
celery_app = Celery('task', broker=BROKER_URL)
redis_client = Redis(host='127.0.0.1', port=6379, db=0, password='000000', decode_responses=True)


@celery_app.task(name="run_query")
def run_complex_query_task(query_id: str):
    """
    一个模拟执行耗时查询的 Celery 任务，使用 Redis 分布式锁。
    """
    # 2. 定义锁的名称，确保对于同一个查询请求，锁的名称是唯一的
    lock_key = f"lock:run_query:{query_id}"

    # 锁的超时时间（秒），应大于任务执行所需的最长时间，防止死锁
    lock_timeout = 60 * 10  # 10 分钟
    # 尝试获取锁的阻塞等待时间（秒），设为0表示不等待，立即返回
    blocking_timeout = 0

    worker_name = os.getpid()  # 用进程ID模拟 Worker 名称

    try:
        # 3. 使用 with 上下文管理器尝试获取锁
        # blocking_timeout=0: 如果锁被占用，会立即抛出 LockError
        with Lock(redis_client, lock_key, timeout=lock_timeout, blocking_timeout=blocking_timeout):

            print(f"[Worker {worker_name}] 成功获取锁 for query {query_id}。")
            print(f"[Worker {worker_name}] 开始执行复杂查询...")

            # --- 模拟核心业务逻辑 ---
            print(f"[Worker {worker_name}] -> 正在连接数据库...")
            time.sleep(2)
            print(f"[Worker {worker_name}] -> 正在执行耗时的 SQL 查询 (query_id: {query_id})...")
            time.sleep(5)
            print(f"[Worker {worker_name}] -> 正在处理查询结果并生成报表...")
            time.sleep(3)
            # --- 核心业务逻辑结束 ---

            print(f"[Worker {worker_name}] 查询 {query_id} 完成！报表已生成。锁将自动释放。")
            # with 语句结束时，锁会自动被安全地释放

    except LockError:
        # 4. 如果未能获取锁，说明有另一个 Worker 正在处理
        print(f"[Worker {worker_name}] 获取锁失败。查询 {query_id} 正在由其他 Worker 执行，本次任务跳过。")
        return  # 直接返回，放弃执行

    except Exception as e:
        print(f"[Worker {worker_name}] 处理查询 {query_id} 时发生未知错误: {e}")
        # 即使发生异常，with 语句也能确保锁被释放（如果已获取）
        raise  # 重新抛出异常，以便 Celery 可以根据配置进行重试


if __name__ == '__main__':
    # 这是一个用于触发任务的脚本
    print("正在向队列发送两个针对同一查询ID的重复任务...")
    query_to_run = f"rates_checking_report_{time.strftime('%Y%m%d')}"
    run_complex_query_task.apply_async(args=[query_to_run])
    run_complex_query_task.apply_async(args=[query_to_run])
    print("任务已发送。请观察 Worker 日志。")

```

#### 四、运行与演示

**启动第一个 Worker**：
   打开一个终端，运行以下命令：

```bash
celery -A tasks worker --loglevel=info -c 1 -n worker1@%h -P eventlet
```


**启动第二个 Worker**：
   **再打开一个新的终端**，运行以下命令，启动第二个 Worker：

```bash
celery -A tasks worker --loglevel=info -c 1 -n worker2@%h -P eventlet
```

现在您有两个独立的 Worker 进程在同时等待任务。

**触发任务**：
   **再打开第三个终端**，运行我们的 Python 脚本来发送任务：

```bash
python tasks.py
```

这个脚本会立即向队列中发送两个完全相同的任务。

```bash
正在向队列发送两个针对同一查询ID的重复任务...
任务已发送。请观察 Worker 日志。
```

**观察结果**：
   您会看到，几乎在同一时间，两个 Worker 都收到了任务。但是，只有一个 Worker 能够成功获取锁并执行业务逻辑，而另一个会因为获取锁失败而直接跳过。

**Worker 1 的日志如下**：

```
[... INFO/MainProcess] Received task: run_query[2cdf6f09-51e2-4c2a-b181-70df4fa28dd5]  
[Worker 27996] 成功获取锁 for query rates_checking_report_20251127。
[Worker 27996] 开始执行复杂查询...
[Worker 27996] -> 正在连接数据库...
[Worker 27996] -> 正在执行耗时的 SQL 查询 (query_id: report_daily_sales_20251127)...
[Worker 27996] -> 正在处理查询结果并生成报表...
[Worker 27996] 查询 report_daily_sales_20251127 完成！报表已生成。锁将自动释放。
[... INFO/MainProcess] Task run_query[2cdf6f09-51e2-4c2a-b181-70df4fa28dd5] succeeded in 10.03099999999904s: None
```

**Worker 2 的日志如下**：

```
[... INFO/MainProcess] Received task: run_query[f61bd4cd-908c-474e-a8cc-1896aed7ec43] 
[Worker 54321] 获取锁失败。查询 report_daily_sales_20231027 正在由其他 Worker 执行，本次任务跳过。
[... INFO/MainProcess] Task run_query[f61bd4cd-908c-474e-a8cc-1896aed7ec43] succeeded in 0.0s: None
```
