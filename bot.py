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
#     print(f'''{update.message.chat['id']}
# {update.message.chat['title']}
# {update.message.chat['type']}''')
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
    
    if text == '/create me':
        chatId = update.message.chat['id']
        userId = update.message.from_user['id']
        firstName = update.message.from_user['first_name']
        username = update.message.from_user['username']
        db = DataBase()
        status = db.createUser(chatId, userId, firstName, username)
        if status == True:
            await update.message.reply_text('Пользователь был успешно добавлен')
        else:
            await update.message.reply_text('Что-то пошло не так')

    if text == '/update me':
        userId = update.message.from_user['id']
        firstName = update.message.from_user['first_name']
        username = update.message.from_user['username']
        db = DataBase()
        status = db.updateUser(userId, firstName, username)
        if status == True:
            await update.message.reply_text('Пользователь был успешно изменен')
        else:
            await update.message.reply_text('Что-то пошло не так')
        
    if '@TikTokDownloaderRusBot' in text:
        await update.message.reply_text(choice(respondsOld))
    if '@all' in text:
        usernames = DataBase().getAllUsernames(update.message.chat['id'])
        await update.message.reply_text(usernames)
def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT, handleMessage))
    app.run_polling(poll_interval=3)

if __name__ == '__main__':
    main()