import asyncio
from datetime import datetime

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient


async def seed_all_tables(uri):
    client = AsyncIOMotorClient(uri)
    db = client.get_database()
    
    # Common IDs to link the tables
    shared_ticket_id = ObjectId()
    shared_user_id = ObjectId()
    shared_room_id = ObjectId()
    shared_company_id = ObjectId()

    # 1. Ticket Data
    ticket = {
        "_id": shared_ticket_id,
        "date": datetime.now(),
        "calledAt": datetime.now(),
        "servedDate": datetime.now(),
        "companyId": shared_company_id,
        "serviceName": "General Inquiry",
        "roomId": shared_room_id,
        "staffId": shared_user_id,
        "ticketNumber": "A-001",
        "sequentialNumber": 1,
        "served": True,
        "updatedAt": datetime.now()
    }

    # 2. User Data (with assignedRooms array)
    user = {
        "_id": shared_user_id,
        "username": "john_doe",
        "email": "john@example.com",
        "phone": "1234567890",
        "password": "hashed_password",
        "role": "staff",
        "assignedRooms": [shared_room_id],
        "updatedAt": datetime.now()
    }

    # 3. Rating Data
    rating = {
        "ticketId": shared_ticket_id,
        "ticketNumber": "A-001",
        "roomId": shared_room_id,
        "roomName": "Room 101",
        "companyId": shared_company_id,
        "companyName": "Tech Corp",
        "userId": shared_user_id,
        "userName": "John Doe",
        "stars": 5,
        "updatedAt": datetime.now()
    }

    # 4. Display Ticket Data
    display_ticket = {
        "ticketId": shared_ticket_id,
        "companyName": "Tech Corp",
        "roomId": shared_room_id,
        "roomName": "Room 101",
        "ticketNumber": "A-001",
        "ticketCreatedAt": datetime.now(),
        "updatedAt": datetime.now()
    }

    # Insert into collections (Matching your Mongoose model names)
    await db.tickets.insert_one(ticket)
    await db.users.insert_one(user)
    await db.ratings.insert_one(rating)
    await db.displaytickets.insert_one(display_ticket)
    
    print(f"✅ All 4 tables seeded in {uri}")

if __name__ == "__main__":
    asyncio.run(seed_all_tables("mongodb://localhost:27018/source_db"))