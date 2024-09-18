from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from tiktok import downloadTikTok, deleteVideo, findLink
from instagram import downloadInstagram
from secrets import choice
from random import randint
from responds import responds, respondsOld
from env import TOKEN
from database import DataBase

async def handleMessage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.edited_message:
        return
    text: str = update.message.text
    user = update.message.from_user
    username = user['username']
    link = findLink(text)
    if link is not None:
        if 'tiktok.com' in link:
            fileName = await downloadTikTok(link)
            if fileName:
                await update.message.reply_video(video=open(fileName, 'rb'))
                deleteVideo(fileName)
            else:
                await update.message.reply_text("Failed to download video.")
        elif 'instagram.com/reel/' in link or 'instagram.com/p/' in link:
            fileName = await downloadInstagram(link)
            print(fileName)
            if fileName:
                await update.message.reply_video(video=open(fileName, 'rb'))
                deleteVideo(fileName)
            else:
                await update.message.reply_text("Failed to download video.")

    if username != 'amialmighty':
        chance = randint(0, 100)
        # print(username + ': ' + str(chance))
        if chance == 69:
            await update.message.reply_text(choice(responds))

    if text == '/create chat':
        chatId = update.message.chat['id']
        title = update.message.chat['title']
        type = update.message.chat['type']
        db = DataBase()
        status = db.createGroupChat(chatId, title, type)
        if status == True:
            await update.message.reply_text('Чат был успешно создан')
        else:
            await update.message.reply_text('Что-то пошло не так')
    
    elif text == '/create me':
        chatId = update.message.chat['id']
        userId = update.message.from_user['id']
        firstName = update.message.from_user['first_name']
        username = update.message.from_user['username']
        db = DataBase()
        status1 = db.createUser(userId, firstName, username)
        status2 = db.addGroupChatToUser(userId, chatId)
        if status1 and status2:
            await update.message.reply_text('Пользователь был успешно добавлен')
        else:
            await update.message.reply_text('Что-то пошло не так')

    elif text == '/update me':
        userId = update.message.from_user['id']
        firstName = update.message.from_user['first_name']
        username = update.message.from_user['username']
        status = DataBase().updateUser(userId, firstName, username)
        if status == True:
            await update.message.reply_text('Пользователь был успешно изменен')
        else:
            await update.message.reply_text('Что-то пошло не так')
    elif text.startswith('/create group '):
        chatId = update.message.chat['id']
        message = DataBase().createGroup(text, chatId)
        await update.message.reply_text(message)

    elif text.startswith('/delete group name:'):
        chatId = update.message.chat['id']
        status = DataBase().deleteGroup(text, chatId)
        if status:
            await update.message.reply_text('Group was succesfully delited')
        else:
            await update.message.reply_text('Something went wrong')

    elif text.startswith('/add to group '):
        chatId = update.message.chat['id']
        message = DataBase().addUsersToGroup(text, chatId)
        await update.message.reply_text(message)

    elif text.startswith('/delete users group '):
        chatId = update.message.chat['id']
        message = DataBase().deleteUsersFromGroup(text, chatId)
        await update.message.reply_text(message)
        
    if '@TikTokDownloaderRusBot' in text:
        await update.message.reply_text(choice(respondsOld))
    print(text)
    if text == '/get_commands@TikTokDownloaderRusBot':
        comandList = """/create chat - Add current chat to bot DB
/create me - Add current user to bot DB
/update me - Update user info in bot DB
/create group name:{name} users:{username},{username} - Add group to chat
/add to group name:{name} users:{username},{username} - Add users to group
/delete group name:{name} - Delete group from chat
/delete users group name:{name} users:{username},{username}- delete users from group"""
        await update.message.reply_text(comandList)

    if '@all' in text:
        usernames = DataBase().getAllUsernames(update.message.chat['id'])
        await update.message.reply_text(usernames)
    elif '@' in text:
        groups = DataBase().getAllGroups(update.message.chat['id'])
        for group in groups:
            if '@'+group['name'] in text:
                groupName = group['name'].replace('@', '')
                chatId = update.message.chat['id']
                currentGroup = DataBase().getGroupByGroupChatIdAndName(chatId, groupName)
                editedText = text.replace('@'+groupName, '')
                respondText = DataBase().getUsernamesByGroup(currentGroup['id'])+'\n'+editedText
                await update.message.reply_text(respondText)
def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT, handleMessage))
    app.run_polling(poll_interval=3)

if __name__ == '__main__':
    main()