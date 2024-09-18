import sqlite3
import re

class DataBase:
    def __init__(self):
        self.conn = sqlite3.connect('main.db')
        self.curr = self.conn.cursor()
        # create group chat table
        self.curr.execute("""
            create table if not exists group_chats(
                id integer,
                title varchar(255),
                type varchar(30)
            )
        """)
        # create users table
        self.curr.execute("""
            create table if not exists users(
                id integer,
                first_name varchar(255),
                username varchar(255)
            )
        """)
        # create many to many between users and group_chats
        self.curr.execute("""
            create table if not exists user_group_chats(
                user_id integer,
                group_chat_id integer,
                foreign key(user_id) references users(id),
                foreign key(group_chat_id) references group_chats(id)
            )
        """)
        # create groups table
        self.curr.execute("""
            create table if not exists groups(
                id integer primary key,
                name varchar(255),
                group_chat_id integer,
                foreign key(group_chat_id) references group_chat(id)
            )
        """)
        # create many to many between users and groups
        self.curr.execute("""
            create table if not exists user_groups(
                user_id integer,
                group_id integer,
                foreign key(user_id) references users(id),
                foreign key(group_id) references groups(id)
            )
        """)


    def getUser(self, id):
        result = self.curr.execute('select * from users where id = ? limit 1', (id,))
        row = result.fetchone()
        if row != None:
            return {
                'id': row[0],
                'first_name': row[1],
                'username': row[2]
            }
        return None
    
    def getUserbyUsername(self, username):
        result = self.curr.execute('select * from users where username = ? limit 1', (username,))
        row = result.fetchone()
        if row != None:
            return {
                'id': row[0],
                'first_name': row[1],
                'username': row[2]
            }
        return None
    
    def createUser(self, id, first_name, username):
        user = self.getUser(id)
        if user == None:
            self.curr.execute('insert into users(id, first_name, username) values(?,?,?)', (id, first_name, username))
            self.conn.commit()
            print('user has been created')
            return True
        print('user was already created')
        return False
    
    def addGroupChatToUser(self, userId, chatId):
        user = self.getUser(userId)
        if user == None:
            print('user was not found')
            return False
        
        if self.isUserAlreadyInGroupChat(userId, chatId):
            print('user already linked to that chat')
            return False
        self.curr.execute('insert into user_group_chats(user_id, group_chat_id) values(?,?)', (userId, chatId))
        self.conn.commit()
        print('user has been added to group chat')
        return True
    
    def createGroupChat(self, chatId, title, type):
        if self.getGroupChat(chatId) == None:
            self.curr.execute('insert into group_chats(id, title, type) values(?,?,?)', (chatId, title, type))
            self.conn.commit()
            print('group chat has been created')
            return True
        print('group chat is already created')        
        return False
    
    def getGroupChat(self, chatId):
        result = self.curr.execute('select * from group_chats where id = ? limit 1', (chatId,))
        row = result.fetchone()
        if row != None:
            return {
                'id':row[0],
                'title':row[1],
                'type':row[2]
            }
        return None
    
    def getAllUsernames(self, chat_id):
        result = self.curr.execute(
        """
        SELECT users.username
        FROM users
        JOIN user_group_chats ON user_group_chats.user_id = users.id
        WHERE user_group_chats.group_chat_id = ?
        """,
        (chat_id,)
    )
        rows = result.fetchall()
        print(rows)
        usernames = ['@' + name[0] for name in rows]
        usernamesString = ', '.join(usernames)
        return usernamesString if rows else 'Something went wrong'
    
    def isUserAlreadyInGroupChat(self, userId, groupChatId):
        result = self.curr.execute('select * from user_group_chats where user_id = ? and group_chat_id = ? limit 1', (userId, groupChatId))
        row = result.fetchone()
        if row != None:
            return True
        return False
    
    def isUserAlreadyInGroup(self, userId, groupId):
        result = self.curr.execute('select * from user_groups where user_id = ? and group_id = ? limit 1', (userId, groupId))
        row = result.fetchone()
        if row != None:
            return True
        return False
    
    def updateUser(self, userId, firstName, username):
        user = self.getUser(userId)
        if user == None:
            print('User with that id does not exists')
            return False
        if user['first_name'] == firstName and user['username'] == username:
            print('There is nothing new about you')
            return False
        self.curr.execute('update users set first_name = ?, username = ? where id = ?', (firstName, username, userId))
        self.conn.commit()
        print('User info has been updated')
        return True
    
    def getGroupByGroupChatIdAndName(self, groupChatId, name):
        result = self.curr.execute('select * from groups where group_chat_id = ? and name = ? limit 1', (groupChatId, name))
        row = result.fetchone()
        if row != None:
            return {
                'id': row[0],
                'name': row[1],
                'groupChatId':row[2]
            }
        return None
    
    def parseCommand(self, command):
            result = {
                'success': False,
                'groupName': None,
                'users': []
            }

            pattern = r"name:(?P<name>\w+)(?: users:(?P<users>\w+(?:,\s*\w+)*))?"
            match = re.match(pattern, command)

            if match:
                result['success'] = True
                result['groupName'] = match.group('name')
                users_match = match.group('users')
                if users_match:
                    result['users'] = re.findall(r"\w+", users_match)
            return result
    
    def createGroup(self, command, groupChatId):
        command = command.replace('@', '')
        command = command.replace('/create group ', '')
        message = ''
        result = self.parseCommand(command)
        print(result)
        group = self.getGroupByGroupChatIdAndName(groupChatId, result['groupName'])
        if group != None:
            message = message+'Group @'+result['groupName']+' is already exists'
        else:
            self.curr.execute('insert into groups(name, group_chat_id) values(?,?)', (result['groupName'], groupChatId))
            self.conn.commit()
            message = message+'Group @'+result['groupName']+' has been created'
            group = self.getGroupByGroupChatIdAndName(groupChatId, result['groupName'])
        query = self.curr.execute('select id from groups where name = ? and group_chat_id = ? limit 1', (result['groupName'], groupChatId))
        groupId = query.fetchone()[0]
        for username in result['users']:
            user = self.getUserbyUsername(username)
            if user == None:
                message = message +'\nUser @' + username + ' was not found'
            else:
                self.curr.execute('insert into user_groups(user_id, group_id) values(?,?)', (user['id'], groupId))
                self.conn.commit()
                message = message+'\nUser @' + username + ' has been added'
        return message
    
    def deleteGroup(self, command, groupChatId):
        command = command.replace('@', '')
        groupName = command.replace('/delete group name:', '')
        groupName = groupName.replace(' ', '')
        group = self.getGroupByGroupChatIdAndName(groupChatId, groupName)
        if group is not None:
            self.curr.execute('delete from user_groups where group_id = ?', (group['id'],))
            self.conn.commit()
            self.curr.execute('delete from groups where id = ?', (group['id'],))
            self.conn.commit()
            print('group has been deleted')
            return True
        else:
            print('group was not found')
            return False

    def addUsersToGroup(self, command, groupChatId):
        command = command.replace('@', '')
        command = command.replace('/add to group ', '')
        print(command)
        result = self.parseCommand(command)
        group = self.getGroupByGroupChatIdAndName(groupChatId, result['groupName'])
        if group is not None:
            message = ''
            for username in result['users']:
                user = self.getUserbyUsername(username)
                if user == None:
                    message = message +'\nUser @' + username + ' was not found'
                else:
                    if self.isUserAlreadyInGroup(user['id'], group['id']):
                        message = message + '\nUser @' + username + ' is already in group'
                    else:
                        self.curr.execute('insert into user_groups(user_id, group_id) values (?, ?)', (user['id'], group['id']))
                        self.conn.commit()
                        message = message+'\nUser @' + username + ' has been added'
            return message
        return 'Group @'+result['groupName']+'was not found'
    
    def deleteUsersFromGroup(self, command, groupChatId):
        command = command.replace('@', '')
        command = command.replace('/delete users group ', '')
        result = self.parseCommand(command)
        group = self.getGroupByGroupChatIdAndName(groupChatId, result['groupName'])
        if group is not None:
            message = ''
            for username in result['users']:
                user = self.getUserbyUsername(username)
                if user == None:
                    message = message +'\nUser @' + username + ' was not found'
                else:
                    self.curr.execute('delete from user_groups where user_id = ? and group_id = ?', (user['id'], group['id']))
                    self.conn.commit()
                    message = message+'\nUser @' + username + ' has been deleted'
            return message
        return 'Group @'+result['groupName']+'was not found'
    
    def getAllGroups(self, groupChatId):
        result = self.curr.execute('select * from groups where group_chat_id = ?', (groupChatId,))
        rows = result.fetchall()
        return [{'id':row[0], 'name':row[1], 'group_chat_id':row[2]} for row in rows] if rows else 'Something went wrong'
    
    def getUsernamesByGroup(self, groupId):
        result = self.curr.execute(
            """
            SELECT users.username
            FROM user_groups
            JOIN users ON user_groups.user_id = users.id
            WHERE user_groups.group_id = ?
            """,
            (groupId,)
        )
        rows = result.fetchall()
        usernames = ['@' + name[0] for name in rows]
        usernamesString = ', '.join(usernames)
        return usernamesString if rows else 'Something went wrong'
    


    


def main():
    CHAT_ID = -4083204401
    db = DataBase()
    db.createGroupChat(1, 'fake', 'group')
    db.createUser(123, 'user', 'rus')
    db.addGroupChatToUser(123, 1)
    db.createUser(124, 'sada', 'gosha')
    db.addGroupChatToUser(124, 1)
    # db.createUser(1646306856, 'qweqwe', 'test')
    # db.addGroupChatToUser(1646306856, CHAT_ID)
    creatGroupString = '/create group name:zek users:@Rus_Shiga,@songoshan '
    addUsersToGroupString = '/group name:gymik users:@rus,@gosha,  @petya'
    deleteGroupString = '/delete group name:zek'
    deleteUsersGroupString = '/delete users group name:gymik users:@rus,@gosha,  @petya'
    addUsersGroupString = '/add to group name:gymik100 users:@gosha'
    queryString = '/create group name:gymik100 users:@rus,  @petya'
    # print(db.deleteGroup(deleteGroupString, CHAT_ID))
    # print(db.createGroup(queryString, CHAT_ID))
    # print(db.addUsersToGroup(addUsersGroupString, CHAT_ID))
    # print(parse_command(queryString))
    # print(db.getUsernamesByGroup(1))
    # db.deleteGroup(deleteGroupString, CHAT_ID)
    # db.createGroup(creatGroupString, CHAT_ID)
    # print(db.getUsernamesByGroup(1))
    print(db.getAllUsernames(1))

if __name__ == '__main__':
    main()