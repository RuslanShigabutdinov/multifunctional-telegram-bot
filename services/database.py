import re

import psycopg

from utils.settings import get_settings


class DataBase:
    def __init__(self) -> None:
        settings = get_settings()
        self.conn = psycopg.connect(
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
            autocommit=True,
        )
        self.curr = self.conn.cursor()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS group_chats(
                id BIGINT PRIMARY KEY,
                title VARCHAR(255),
                type VARCHAR(30)
            )
            """
        )
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS users(
                id BIGINT PRIMARY KEY,
                first_name VARCHAR(255),
                username VARCHAR(255)
            )
            """
        )
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS user_group_chats(
                user_id BIGINT,
                group_chat_id BIGINT,
                PRIMARY KEY (user_id, group_chat_id),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(group_chat_id) REFERENCES group_chats(id) ON DELETE CASCADE
            )
            """
        )
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS groups(
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                group_chat_id BIGINT REFERENCES group_chats(id) ON DELETE CASCADE
            )
            """
        )
        self.curr.execute(
            """
            CREATE TABLE IF NOT EXISTS user_groups(
                user_id BIGINT,
                group_id INTEGER,
                PRIMARY KEY (user_id, group_id),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
            )
            """
        )

    def getUser(self, id):
        self.curr.execute("SELECT * FROM users WHERE id = %s LIMIT 1", (id,))
        row = self.curr.fetchone()
        if row is not None:
            return {"id": row[0], "first_name": row[1], "username": row[2]}
        return None

    def getUserbyUsername(self, username):
        self.curr.execute(
            "SELECT * FROM users WHERE username = %s LIMIT 1", (username,)
        )
        row = self.curr.fetchone()
        if row is not None:
            return {"id": row[0], "first_name": row[1], "username": row[2]}
        return None

    def createUser(self, id, first_name, username):
        user = self.getUser(id)
        if user is None:
            self.curr.execute(
                "INSERT INTO users(id, first_name, username) VALUES(%s,%s,%s)",
                (id, first_name, username),
            )
            print("user has been created")
            return True
        print("user was already created")
        return False

    def addGroupChatToUser(self, userId, chatId):
        user = self.getUser(userId)
        if user is None:
            print("user was not found")
            return False

        if self.isUserAlreadyInGroupChat(userId, chatId):
            print("user already linked to that chat")
            return False
        self.curr.execute(
            "INSERT INTO user_group_chats(user_id, group_chat_id) VALUES(%s,%s)",
            (userId, chatId),
        )
        print("user has been added to group chat")
        return True

    def createGroupChat(self, chatId, title, type):
        if self.getGroupChat(chatId) is None:
            self.curr.execute(
                "INSERT INTO group_chats(id, title, type) VALUES(%s,%s,%s)",
                (chatId, title, type),
            )
            print("group chat has been created")
            return True
        print("group chat is already created")
        return False

    def getGroupChat(self, chatId):
        self.curr.execute(
            "SELECT * FROM group_chats WHERE id = %s LIMIT 1", (chatId,)
        )
        row = self.curr.fetchone()
        if row is not None:
            return {"id": row[0], "title": row[1], "type": row[2]}
        return None

    def getAllUsernames(self, chat_id):
        self.curr.execute(
            """
            SELECT users.username
            FROM users
            JOIN user_group_chats ON user_group_chats.user_id = users.id
            WHERE user_group_chats.group_chat_id = %s
            """,
            (chat_id,),
        )
        rows = self.curr.fetchall()
        print(rows)
        usernames = ["@" + name[0] for name in rows]
        usernamesString = ", ".join(usernames)
        return usernamesString if rows else "Something went wrong"

    def isUserAlreadyInGroupChat(self, userId, groupChatId):
        self.curr.execute(
            "SELECT 1 FROM user_group_chats WHERE user_id = %s AND group_chat_id = %s LIMIT 1",
            (userId, groupChatId),
        )
        row = self.curr.fetchone()
        return row is not None

    def isUserAlreadyInGroup(self, userId, groupId):
        self.curr.execute(
            "SELECT 1 FROM user_groups WHERE user_id = %s AND group_id = %s LIMIT 1",
            (userId, groupId),
        )
        row = self.curr.fetchone()
        return row is not None

    def updateUser(self, userId, firstName, username):
        user = self.getUser(userId)
        if user is None:
            print("User with that id does not exists")
            return False
        if user["first_name"] == firstName and user["username"] == username:
            print("There is nothing new about you")
            return False
        self.curr.execute(
            "UPDATE users SET first_name = %s, username = %s WHERE id = %s",
            (firstName, username, userId),
        )
        print("User info has been updated")
        return True

    def getGroupByGroupChatIdAndName(self, groupChatId, name):
        self.curr.execute(
            "SELECT * FROM groups WHERE group_chat_id = %s AND name = %s LIMIT 1",
            (groupChatId, name),
        )
        row = self.curr.fetchone()
        if row is not None:
            return {"id": row[0], "name": row[1], "groupChatId": row[2]}
        return None

    def parseCommand(self, command):
        result = {"success": False, "groupName": None, "users": []}

        pattern = r"name:(?P<name>\w+)(?: users:(?P<users>\w+(?:,\s*\w+)*))?"
        match = re.match(pattern, command)

        if match:
            result["success"] = True
            result["groupName"] = match.group("name")
            users_match = match.group("users")
            if users_match:
                result["users"] = re.findall(r"\w+", users_match)
        return result

    def createGroup(self, command, groupChatId):
        command = command.replace("@", "")
        command = command.replace("/create group ", "")
        message = ""
        result = self.parseCommand(command)
        print(result)
        group = self.getGroupByGroupChatIdAndName(groupChatId, result["groupName"])
        if group is not None:
            message = message + "Group @" + result["groupName"] + " is already exists"
        else:
            self.curr.execute(
                "INSERT INTO groups(name, group_chat_id) VALUES(%s,%s)",
                (result["groupName"], groupChatId),
            )
            message = message + "Group @" + result["groupName"] + " has been created"
            group = self.getGroupByGroupChatIdAndName(groupChatId, result["groupName"])
        self.curr.execute(
            "SELECT id FROM groups WHERE name = %s AND group_chat_id = %s LIMIT 1",
            (result["groupName"], groupChatId),
        )
        groupId_row = self.curr.fetchone()
        groupId = groupId_row[0] if groupId_row else None
        for username in result["users"]:
            user = self.getUserbyUsername(username)
            if user is None:
                message = message + "\nUser @" + username + " was not found"
            else:
                self.curr.execute(
                    "INSERT INTO user_groups(user_id, group_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",
                    (user["id"], groupId),
                )
                message = message + "\nUser @" + username + " has been added"
        return message

    def deleteGroup(self, command, groupChatId):
        command = command.replace("@", "")
        groupName = command.replace("/delete group name:", "")
        groupName = groupName.replace(" ", "")
        group = self.getGroupByGroupChatIdAndName(groupChatId, groupName)
        if group is not None:
            self.curr.execute("DELETE FROM user_groups WHERE group_id = %s", (group["id"],))
            self.curr.execute("DELETE FROM groups WHERE id = %s", (group["id"],))
            print("group has been deleted")
            return True
        else:
            print("group was not found")
            return False

    def addUsersToGroup(self, command, groupChatId):
        command = command.replace("@", "")
        command = command.replace("/add to group ", "")
        print(command)
        result = self.parseCommand(command)
        group = self.getGroupByGroupChatIdAndName(groupChatId, result["groupName"])
        if group is not None:
            message = ""
            for username in result["users"]:
                user = self.getUserbyUsername(username)
                if user is None:
                    message = message + "\nUser @" + username + " was not found"
                else:
                    if self.isUserAlreadyInGroup(user["id"], group["id"]):
                        message = message + "\nUser @" + username + " is already in group"
                    else:
                        self.curr.execute(
                            "INSERT INTO user_groups(user_id, group_id) VALUES (%s, %s)",
                            (user["id"], group["id"]),
                        )
                        message = message + "\nUser @" + username + " has been added"
            return message
        return "Group @" + result["groupName"] + " was not found"

    def deleteUsersFromGroup(self, command, groupChatId):
        command = command.replace("@", "")
        command = command.replace("/delete users group ", "")
        result = self.parseCommand(command)
        group = self.getGroupByGroupChatIdAndName(groupChatId, result["groupName"])
        if group is not None:
            message = ""
            for username in result["users"]:
                user = self.getUserbyUsername(username)
                if user is None:
                    message = message + "\nUser @" + username + " was not found"
                else:
                    self.curr.execute(
                        "DELETE FROM user_groups WHERE user_id = %s AND group_id = %s",
                        (user["id"], group["id"]),
                    )
                    message = message + "\nUser @" + username + " has been deleted"
            return message
        return "Group @" + result["groupName"] + " was not found"

    def getAllGroups(self, groupChatId):
        self.curr.execute("SELECT * FROM groups WHERE group_chat_id = %s", (groupChatId,))
        rows = self.curr.fetchall()
        return (
            [{"id": row[0], "name": row[1], "group_chat_id": row[2]} for row in rows]
            if rows
            else "Something went wrong"
        )

    def getUsernamesByGroup(self, groupId):
        self.curr.execute(
            """
            SELECT users.username
            FROM user_groups
            JOIN users ON user_groups.user_id = users.id
            WHERE user_groups.group_id = %s
            """,
            (groupId,),
        )
        rows = self.curr.fetchall()
        usernames = ["@" + name[0] for name in rows]
        usernamesString = ", ".join(usernames)
        return usernamesString if rows else "Something went wrong"

    def getGroupChatsForUser(self, userId):
        self.curr.execute(
            """
            SELECT gc.id, gc.title, gc.type
            FROM group_chats gc
            JOIN user_group_chats ugc ON ugc.group_chat_id = gc.id
            WHERE ugc.user_id = %s
            ORDER BY gc.title
            """,
            (userId,),
        )
        rows = self.curr.fetchall()
        return [{"id": row[0], "title": row[1], "type": row[2]} for row in rows]
