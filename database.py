import sqlite3

class DataBase:
    def __init__(self):
        self.conn = sqlite3.connect('main.db')
        self.curr = self.conn.cursor()
        self.curr.execute("""
            create table if not exists group_chats(
                id integer primary key,
                chat_id integer,
                title varchar(255),
                type varchar(30)
            )
        """)
        self.curr.execute("""
            create table if not exists users(
                id integer primary key,
                group_chat_id integer,
                user_id integer,
                first_name varchar(255),
                username varchar(255),
                foreign key(group_chat_id) references group_chat(id)
            )
        """)
    def createGroupChat(self, chat_id, title, type):
        if self.getGroupChatId(chat_id) == None:
            self.curr.execute('insert into group_chats(chat_id, title, type) values(?,?,?)', (chat_id, title, type))
            self.conn.commit()
            print('group chat has been created')
            return True
        return False
    
    def getGroupChatId(self, chat_id):
        result = self.curr.execute('select (id) from group_chats where chat_id = ? limit 1', (chat_id,))
        row = result.fetchone()
        if row != None:
            return row[0]
        return None
    
    def getUserId(self, user_id):
        result = self.curr.execute('select (id) from users where user_id = ? limit 1', (user_id,))
        row = result.fetchone()
        if row != None:
            return row[0]
        return None
    
    def getAllUserIds(self, user_id):
        result = self.curr.execute('select (id) from users where user_id = ?', (user_id,))
        rows = result.fetchall()
        if rows != None:
            ids = []
            for id in rows:
                ids.append(id[0])
            return ids
        return None
    
    def getAllUsernames(self, chat_id):
        groupChatId = self.getGroupChatId(chat_id)
        result = self.curr.execute('select (username) from users where group_chat_id = ?', (groupChatId,))
        rows = result.fetchall()
        usernames = []
        for name in rows:
            usernames.append('@'+name[0])
        usernamesString = ', '.join(usernames)
        if rows != None:
            return usernamesString
        return 'Что-то пошло не так'
        
    
    def isUserAlreadyInGroupChat(self, user_id, groupChatId):
        result = self.curr.execute('select (id) from users where user_id = ? and group_chat_id = ? limit 1', (user_id, groupChatId))
        row = result.fetchone()
        if row != None:
            return True
        return False

    def createUser(self, chat_id, user_id, first_name, username):
        group_chat_id = self.getGroupChatId(chat_id)
        if not self.isUserAlreadyInGroupChat(user_id, group_chat_id):
            self.curr.execute('insert into users(group_chat_id, user_id, first_name, username) values(?,?,?,?)', (group_chat_id, user_id, first_name, username))
            self.conn.commit()
            print('user has been created')
            return True
        return False
    
    def updateUser(self, user_id, first_name, username):
        ids = self.getAllUserIds(user_id)
        for id in ids:
            self.curr.execute('update users set first_name = ?, username = ? where id = ?', (first_name, username, id))
            self.conn.commit()
        print('all user info has been updated')
        return True

        




def main():
    db = DataBase()
    # db.createGroupChat(-4083204401, 'friends-crossover', 'group')
    # db.createUser(-602765732, 1646306858, 'Gosha', 'songoshan')
    db.curr.execute('update users set first_name = "rusio" where id = 1')
    db.conn.commit()

    # print(db.getGroupChatId(-4083204401))

if __name__ == '__main__':
    main()