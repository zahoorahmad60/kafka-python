# 📚 KAFKA FOR BEGINNERS - Like a School Message Board!
#
# Imagine your school has a message board where:
# - Teachers post announcements (PRODUCER)
# - Students read announcements (CONSUMER)
# - The board has different sections like "homework", "events" (TOPICS)

# First, install this: pip install kafka-python

from kafka import KafkaProducer, KafkaConsumer
import json
import time

# 🏫 PART 1: TEACHER POSTING MESSAGES (Producer)
print("🏫 SCHOOL MESSAGE BOARD SYSTEM")
print("=" * 40)

def teacher_posts_message():
    """
    This is like a teacher writing on the message board
    """
    print("👩‍🏫 Teacher is posting messages...")
    
    # Create a "teacher" (producer)
    teacher = KafkaProducer(
        bootstrap_servers=['localhost:9092'],  # Where the message board is
        value_serializer=lambda message: message.encode('utf-8')  # Convert text to computer format
    )
    
    # Messages the teacher wants to post
    messages = [
        "📚 Homework: Math chapter 5, problems 1-10",
        "🎉 School picnic on Friday!",
        "📝 Science test next Monday",
        "🏃‍♂️ Sports day registration open",
        "📖 Library will be closed tomorrow"
    ]
    
    # Post each message
    for i, message in enumerate(messages, 1):
        print(f"   Posting message {i}: {message}")
        
        # Send message to "school-announcements" board
        teacher.send('school-announcements', message)
        
        time.sleep(1)  # Wait 1 second between messages
    
    # Make sure all messages are posted
    teacher.flush()
    teacher.close()
    print("   ✅ All messages posted!\n")

# 🎓 PART 2: STUDENT READING MESSAGES (Consumer)
def student_reads_messages():
    """
    This is like a student checking the message board
    """
    print("🎓 Student is checking the message board...")
    
    # Create a "student" (consumer)
    student = KafkaConsumer(
        'school-announcements',  # Which board to check
        bootstrap_servers=['localhost:9092'],  # Where the message board is
        value_deserializer=lambda message: message.decode('utf-8'),  # Convert back to readable text
        auto_offset_reset='earliest',  # Start from the first message
        consumer_timeout_ms=5000  # Stop checking after 5 seconds if no new messages
    )
    
    print("   👀 Reading messages...")
    
    message_count = 0
    try:
        # Check each message on the board
        for message in student:
            message_count += 1
            print(f"   📋 Message {message_count}: {message.value}")
            
    except:
        print("   📚 Finished reading all messages!")
    
    student.close()
    print(f"   ✅ Student read {message_count} messages\n")

# 🎯 SIMPLE CHAT EXAMPLE
def simple_chat():
    """
    Even simpler - like sending a single message to a friend
    """
    print("💬 Simple Chat Example")
    print("Sending a message from Alice to Bob...")
    
    # Alice sends a message
    alice = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda msg: msg.encode('utf-8')
    )
    
    # Send one simple message
    message = "Hi Bob! Want to play football after school?"
    alice.send('chat', message)
    alice.flush()
    alice.close()
    print(f"   Alice sent: {message}")
    
    # Bob reads the message
    time.sleep(1)  # Wait a moment
    
    bob = KafkaConsumer(
        'chat',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda msg: msg.decode('utf-8'),
        auto_offset_reset='earliest',
        consumer_timeout_ms=3000
    )
    
    print("   Bob checking messages...")
    try:
        for msg in bob:
            print(f"   Bob received: {msg.value}")
            break  # Just read one message
    except:
        print("   No messages found")
    
    bob.close()
    print("   ✅ Chat example done!\n")

# 🚀 RUN EVERYTHING
def main():
    print("🌟 Welcome to Kafka - Super Simple Version!")
    print("This is like having a digital message board at school\n")
    
    print("📝 What we'll do:")
    print("1. Teacher posts messages (Producer)")
    print("2. Student reads messages (Consumer)")  
    print("3. Simple chat between friends\n")
    
    print("⚠️  Make sure Kafka is running on your computer first!")
    print("If you see errors, ask your teacher to help start Kafka\n")
    
    try:
        # Run examples
        teacher_posts_message()
        student_reads_messages()
        simple_chat()
        
        print("🎉 Great job! You just learned Kafka!")
        print("\n💡 What happened:")
        print("✓ Producer = Someone who SENDS messages")
        print("✓ Consumer = Someone who READS messages")
        print("✓ Topic = The name of the message board (like 'homework' or 'sports')")
        print("✓ Kafka = The system that handles all the messages")
        
    except Exception as e:
        print(f"😅 Oops! Something went wrong: {e}")
        print("\n🔧 This usually means:")
        print("- Kafka is not running")
        print("- Wrong server address")
        print("- Ask your teacher for help!")

if __name__ == "__main__":
    main()

# 📚 BONUS: What is Kafka really used for?
"""
🌟 Real world examples where Kafka is used:

1. 📱 WhatsApp - sending messages between phones
2. 🛒 Amazon - tracking what you buy
3. 🚗 Uber - tracking where cars are
4. 📺 Netflix - recommending movies
5. 🏦 Banks - processing payments

Kafka is like a super-fast, reliable postal service for computers!
It helps different computer programs send messages to each other.

Think of it as:
- Post Office = Kafka
- Letters = Messages  
- Mailboxes = Topics
- People sending letters = Producers
- People reading letters = Consumers
"""