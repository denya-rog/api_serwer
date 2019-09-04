db.createUser(
        {
            user: "admin",
            pwd: "admin",
            roles: [
                {
                    role: "readWrite",
                    db: "some_db"
                }
            ]
        }
);

db.createCollection("new");
db.students.insertOne({ name: "Jon", age: "20" });

