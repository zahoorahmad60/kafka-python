# Kafka Python Examples - Complete Guide for Python Developers
# 
# What is Kafka?
# Apache Kafka is a distributed streaming platform that acts like a high-performance message queue.
# Think of it as a postal system where:
# - Producers send messages (like writing letters)
# - Topics are like mailboxes (where messages are stored)
# - Consumers read messages (like reading letters from mailboxes)

# First, install the required library:
# pip install kafka-python

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time
import threading
from datetime import datetime

# =============================================================================
# EXAMPLE 1: BASIC PRODUCER (Sending Messages)
# =============================================================================

def basic_producer_example():
    """
    A Producer sends messages to a Kafka topic.
    Think of this like posting messages to a message board.
    """
    print("🚀 EXAMPLE 1: Basic Producer")
    
    # Create a producer instance
    # bootstrap_servers: List of Kafka brokers (like server addresses)
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],  # Default Kafka server
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convert Python objects to JSON
    )
    
    # Send some messages to a topic called 'my-topic'
    messages = [
        {"user": "alice", "message": "Hello Kafka!", "timestamp": datetime.now().isoformat()},
        {"user": "bob", "message": "Learning Kafka with Python", "timestamp": datetime.now().isoformat()},
        {"user": "charlie", "message": "This is message #3", "timestamp": datetime.now().isoformat()}
    ]
    
    for i, msg in enumerate(messages):
        # Send message to topic
        future = producer.send('my-topic', value=msg)
        print(f"  📤 Sent message {i+1}: {msg}")
        time.sleep(1)  # Wait 1 second between messages
    
    # Make sure all messages are sent
    producer.flush()
    producer.close()
    print("  ✅ All messages sent!\n")

# =============================================================================
# EXAMPLE 2: BASIC CONSUMER (Reading Messages)
# =============================================================================

def basic_consumer_example():
    """
    A Consumer reads messages from a Kafka topic.
    Think of this like checking your mailbox for new letters.
    """
    print("📨 EXAMPLE 2: Basic Consumer")
    
    # Create a consumer instance
    consumer = KafkaConsumer(
        'my-topic',  # Topic to subscribe to
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Convert JSON back to Python objects
        auto_offset_reset='earliest',  # Start from beginning if no saved position
        consumer_timeout_ms=5000  # Stop after 5 seconds of no messages
    )
    
    print("  📬 Waiting for messages...")
    
    try:
        for message in consumer:
            print(f"  📩 Received: {message.value}")
            print(f"      Topic: {message.topic}")
            print(f"      Partition: {message.partition}")
            print(f"      Offset: {message.offset}")
    except Exception as e:
        print(f"  ⏰ Consumer timeout or finished")
    
    consumer.close()
    print("  ✅ Consumer closed\n")

# =============================================================================
# EXAMPLE 3: PRODUCER WITH PARTITIONS AND KEYS
# =============================================================================

def advanced_producer_example():
    """
    Advanced producer that demonstrates:
    - Message keys (for partitioning)
    - Partitions (like having multiple mailboxes)
    - Callbacks (confirmation when message is sent)
    """
    print("🎯 EXAMPLE 3: Advanced Producer with Keys and Partitions")
    
    def on_send_success(record_metadata):
        print(f"      ✅ Message sent to topic: {record_metadata.topic}")
        print(f"         Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
    
    def on_send_error(excp):
        print(f"      ❌ Message failed to send: {excp}")
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Messages with keys - messages with same key go to same partition
    keyed_messages = [
        ("user-alice", {"user": "alice", "action": "login", "timestamp": datetime.now().isoformat()}),
        ("user-bob", {"user": "bob", "action": "purchase", "amount": 99.99}),
        ("user-alice", {"user": "alice", "action": "logout", "timestamp": datetime.now().isoformat()}),
        ("user-charlie", {"user": "charlie", "action": "signup", "email": "charlie@example.com"}),
    ]
    
    for key, message in keyed_messages:
        print(f"  📤 Sending message with key '{key}': {message}")
        future = producer.send('user-events', key=key, value=message)
        # Add callbacks
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
        time.sleep(0.5)
    
    producer.flush()
    producer.close()
    print("  ✅ Advanced producer finished\n")

# =============================================================================
# EXAMPLE 4: CONSUMER GROUPS
# =============================================================================

def consumer_group_example(group_id, consumer_name):
    """
    Consumer Groups allow multiple consumers to work together.
    Like having multiple people checking different mailboxes.
    """
    print(f"👥 EXAMPLE 4: Consumer Group Member - {consumer_name}")
    
    consumer = KafkaConsumer(
        'user-events',
        bootstrap_servers=['localhost:9092'],
        group_id=group_id,  # All consumers with same group_id work together
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=8000
    )
    
    print(f"  👤 {consumer_name} waiting for messages...")
    
    try:
        for message in consumer:
            print(f"  👤 {consumer_name} received: {message.value}")
            print(f"      From partition: {message.partition}")
            # Simulate processing time
            time.sleep(1)
    except Exception as e:
        print(f"  👤 {consumer_name} finished")
    
    consumer.close()

# =============================================================================
# EXAMPLE 5: REAL-WORLD SCENARIO - ORDER PROCESSING SYSTEM
# =============================================================================

class OrderProcessingSystem:
    """
    Real-world example: E-commerce order processing system
    """
    
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    def place_order(self, customer_id, items):
        """Simulate placing an order"""
        order = {
            "order_id": f"ORD-{int(time.time())}",
            "customer_id": customer_id,
            "items": items,
            "total": sum(item["price"] * item["quantity"] for item in items),
            "status": "pending",
            "timestamp": datetime.now().isoformat()
        }
        
        print(f"  🛒 Order placed: {order['order_id']} by customer {customer_id}")
        
        # Send to orders topic with customer_id as key (for partitioning)
        self.producer.send('orders', key=customer_id, value=order)
        
        return order
    
    def order_processor_consumer(self, processor_name):
        """Process orders from the queue"""
        consumer = KafkaConsumer(
            'orders',
            bootstrap_servers=['localhost:9092'],
            group_id='order-processors',  # Multiple processors can work together
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            consumer_timeout_ms=10000
        )
        
        print(f"  🏭 {processor_name} started processing orders...")
        
        try:
            for message in consumer:
                order = message.value
                print(f"  🏭 {processor_name} processing order: {order['order_id']}")
                print(f"      Customer: {order['customer_id']}, Total: ${order['total']:.2f}")
                
                # Simulate processing time
                time.sleep(2)
                
                # Send processed order to another topic
                processed_order = order.copy()
                processed_order["status"] = "processed"
                processed_order["processed_by"] = processor_name
                processed_order["processed_at"] = datetime.now().isoformat()
                
                self.producer.send('processed-orders', 
                                 key=order['customer_id'], 
                                 value=processed_order)
                
                print(f"  ✅ {processor_name} completed order: {order['order_id']}")
        
        except Exception as e:
            print(f"  🏭 {processor_name} finished processing")
        
        consumer.close()

# =============================================================================
# EXAMPLE 6: TOPIC MANAGEMENT
# =============================================================================

def manage_topics():
    """Create and manage Kafka topics programmatically"""
    print("📋 EXAMPLE 6: Topic Management")
    
    # Create admin client
    admin_client = KafkaAdminClient(
        bootstrap_servers=['localhost:9092']
    )
    
    # Define topics to create
    topics_to_create = [
        NewTopic(name="my-topic", num_partitions=3, replication_factor=1),
        NewTopic(name="user-events", num_partitions=4, replication_factor=1),
        NewTopic(name="orders", num_partitions=2, replication_factor=1),
        NewTopic(name="processed-orders", num_partitions=2, replication_factor=1),
    ]
    
    try:
        # Create topics
        fs = admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print(f"  ✅ Topic {topic} created successfully")
            except TopicAlreadyExistsError:
                print(f"  ℹ️  Topic {topic} already exists")
            except Exception as e:
                print(f"  ❌ Failed to create topic {topic}: {e}")
    
    except Exception as e:
        print(f"  ❌ Error in topic creation: {e}")
    
    # List existing topics
    try:
        metadata = admin_client.list_consumer_groups()
        print(f"  📋 Available consumer groups: {len(metadata)}")
    except Exception as e:
        print(f"  ⚠️  Could not list consumer groups: {e}")
    
    admin_client.close()
    print("  ✅ Topic management completed\n")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def run_all_examples():
    """Run all examples in sequence"""
    print("=" * 60)
    print("🐍 KAFKA PYTHON EXAMPLES - COMPLETE TUTORIAL")
    print("=" * 60)
    
    print("📝 Before running these examples:")
    print("1. Make sure Kafka is running on localhost:9092")
    print("2. Install kafka-python: pip install kafka-python")
    print("3. These examples assume a single-broker setup\n")
    
    # Step 1: Create topics
    manage_topics()
    
    # Step 2: Basic producer
    basic_producer_example()
    
    # Step 3: Basic consumer
    basic_consumer_example()
    
    # Step 4: Advanced producer
    advanced_producer_example()
    
    # Step 5: Consumer groups (run multiple consumers)
    print("👥 EXAMPLE 4: Starting Consumer Group")
    print("  (Running 2 consumers in the same group)")
    
    # Create threads for multiple consumers
    consumer_threads = []
    for i in range(2):
        thread = threading.Thread(
            target=consumer_group_example, 
            args=("my-consumer-group", f"Consumer-{i+1}")
        )
        consumer_threads.append(thread)
        thread.start()
    
    # Wait for consumer threads to finish
    for thread in consumer_threads:
        thread.join()
    
    print("  ✅ Consumer group example completed\n")
    
    # Step 6: Real-world example
    print("🛒 EXAMPLE 5: Real-world Order Processing System")
    
    order_system = OrderProcessingSystem()
    
    # Place some orders
    orders = [
        ("CUST-001", [{"item": "laptop", "price": 999.99, "quantity": 1}]),
        ("CUST-002", [{"item": "mouse", "price": 29.99, "quantity": 2}]),
        ("CUST-001", [{"item": "keyboard", "price": 79.99, "quantity": 1}]),
    ]
    
    for customer_id, items in orders:
        order_system.place_order(customer_id, items)
        time.sleep(0.5)
    
    # Start order processors
    processor_threads = []
    for i in range(2):
        thread = threading.Thread(
            target=order_system.order_processor_consumer,
            args=(f"Processor-{i+1}",)
        )
        processor_threads.append(thread)
        thread.start()
    
    # Wait for processors to finish
    for thread in processor_threads:
        thread.join()
    
    order_system.producer.close()
    print("  ✅ Order processing system example completed\n")
    
    print("🎉 All examples completed!")
    print("\n💡 Key Takeaways:")
    print("- Producers send messages to topics")
    print("- Consumers read messages from topics") 
    print("- Topics are divided into partitions for scalability")
    print("- Consumer groups allow parallel processing")
    print("- Message keys determine which partition a message goes to")
    print("- Kafka is great for building real-time data pipelines")

if __name__ == "__main__":
    run_all_examples()