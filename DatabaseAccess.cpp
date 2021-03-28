#include "DatabaseAccess.h"

// callback function for getting users list
int callBackGetUsers(void* data, int argc, char** argv, char** azColName)
{
    std::list<User>* usersList = (std::list<User>*)data; // will point to the parameter data () this is the output
    User tempUser(0, ""); // will store the temp user that will be pushed into usersList in the end

    // this loop will fill all tempUser fields
    for (int i = 0; i < argc; i++)
    {
        if (std::string(azColName[i]) == "ID") // if collum name = id
        {
            tempUser.setId(std::atoi(argv[i])); // set id to temp user
        }
        else // if else it means that collum name is 'name'
        {
            tempUser.setName(argv[i]); // set name to temp user
        }
    }

    usersList->push_back(tempUser); // push user to the users list after filling the fields

    return 0;
}

// this function will return answer from db in case that is a single number
int callBackGetInt(void* data, int argc, char** argv, char** azColName)
{
    int* result = (int*)data; // that will be the output for function
    *result = std::atoi(argv[0]); // take the only collum in answer

    return 0;
}

// callback function for getting albums list
int callBackGetAlbums(void* data, int argc, char** argv, char** azColName)
{
    std::list<Album>* albumsList = (std::list<Album>*)data; // will point to the parameter data () this is the output
    Album tempAlbum; // will store the temp album that will be pushed into albumsList in the end

    // this loop will fill all tempAlbum fields
    for (int i = 0; i < argc; i++)
    {
        if (std::string(azColName[i]) == "NAME") // if collum name is 'NAME'
        {
            tempAlbum.setName(argv[i]); // set name for temp album
        }
        else if (std::string(azColName[i]) == "USER_ID") // if collum name is 'USER_ID'
        {
            tempAlbum.setOwner(std::atoi(argv[i])); // set owner id for temp album
        }
        else if (std::string(azColName[i]) == "CREATION_DATE") // if collum name is 'CRAETION_DATE'
        {
            tempAlbum.setCreationDate(argv[i]); // set creation date for temp album
        }
    }

    albumsList->push_back(tempAlbum); // push album to the albums list

    return 0;
}

// callback function for getting pictures list
int callBackGetPictures(void* data, int argc, char** argv, char** azColName)
{
    std::list<Picture>* picturesList = (std::list<Picture>*)data; // will point to the parameter data () this is the output
    Picture tempPicture(0, ""); // will store the temp picture that will be pushed into picturesList in the end

    // this loop will fill all tempPicture fields
    for (int i = 0; i < argc; i++)
    {
        if (std::string(azColName[i]) == "NAME") // if collum name is 'NAME'
        {
            tempPicture.setName(argv[i]); // set name for temp picture
        }
        else if (std::string(azColName[i]) == "LOCATION")  // if collum name is 'LOCATION'
        {
            tempPicture.setPath(argv[i]); // set location (file path) for temp picture
        }
        else if (std::string(azColName[i]) == "CRATION_DATE") // if collum name is 'CREATION_DATE'
        {
            tempPicture.setCreationDate(argv[i]); // set creation date for temp picture
        }
        else if (std::string(azColName[i]) == "ID") // if collum name is 'ID'
        {
            tempPicture.setId(std::atoi(argv[i])); // set temp picture id
        }
    }

    picturesList->push_back(tempPicture); // push the temp picture with filled fields to output

    return 0;
}

// callback for amount of tags
int callbackGetTags(void* data, int argc, char** argv, char** azColName)
{
    std::list<std::pair<int, int>>* tagsList = (std::list<std::pair<int, int>>*)data; // will point to the parameter data () this is the output (first in pair is USER_ID, second - PICTURE_ID)
    std::pair<int, int> tempTag; // will store the tempTag for each running of callback

    // run on every collum
    for (int i = 0; i < argc; i++)
    {
        if (std::string(azColName[i]) == "USER_ID") // if collum name is USER_ID 
        {
            tempTag.first = std::atoi(argv[i]); // set first element of pair
        }
        else if (std::string(azColName[i]) == "PICTURE_ID") // if collum name is PICTURE_ID 
        {
            tempTag.second = std::atoi(argv[i]); // set second element of pair
        }
    }

    tagsList->push_back(tempTag); // push filled tempTag to the output

    return 0;
}

// will open and create the database
bool DatabaseAccess::open()
{
    std::string dbFileName = "MyDB.sqlite";

    int doesFileExist = _access(dbFileName.c_str(), 0); // check if db is already exist (before trying to create)

    int res = sqlite3_open(dbFileName.c_str(), &db); // open the db
    if (res != SQLITE_OK) // if failed to open db
    {
        db = nullptr;
        std::cout << "Failed to open DB" << std::endl;
    }
    else // if not failed
    {
        std::cout << "Succsess!\n";
    }


    if (doesFileExist == 0) // if DB elready exist
    {
        std::cout << "DB already exist\n";
    }

    else // if not, create teh tables
    {
        // create users table
        std::string sqlStatement = "CREATE TABLE USERS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL); ";
        sendSqlStatement(sqlStatement, nullptr, nullptr);

        // create phone albums table
        sqlStatement = "CREATE TABLE ALBUMS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL, CREATION_DATE DATE NOT NULL, USER_ID INTEGER NOT NULL, FOREIGN KEY(USER_ID) REFERENCES USERS(ID))";
        sendSqlStatement(sqlStatement, nullptr, nullptr);

        // create pictures table
        sqlStatement = "CREATE TABLE PICTURES (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL, LOCATION TEXT NOT NULL, CREATION_DATE DATE NOT NULL, ALBUM_ID INTEGER NOT NULL, FOREIGN KEY(ALBUM_ID) REFERENCES ALBUMS(ID))";
        sendSqlStatement(sqlStatement, nullptr, nullptr);

        // create tags table
        sqlStatement = "CREATE TABLE TAGS (ID INTEGER PRIMARY KEY AUTOINCREMENT, PICTURE_ID INTEGER NOT NULL, USER_ID INTEGER NOT NULL, FOREIGN KEY(PICTURE_ID) REFERENCES PICTURES(ID), FOREIGN KEY(USER_ID) REFERENCES USERS(ID))";
        sendSqlStatement(sqlStatement, nullptr, nullptr);
    }

    return true;
}



// will close the database
void DatabaseAccess::close()
{
    sqlite3_close(this->db);
    db = nullptr;
}

// this function should clear the data saved by class
void DatabaseAccess::clear()
{
    // but there is nothing to clear all the data is saved in the database and not the class
    // class task is to take the data from the database
}

// will delete album by name
void DatabaseAccess::deleteAlbum(const std::string& albumName, int userId)
{
    // first delete pictures in album
    std::string sqlStatement = "DELETE FROM PICTURES WHERE ALBUM_ID = (SELECT ID FROM ALBUMS WHERE NAME = '" + albumName + "' AND USER_ID = " + std::to_string(userId) + ");";
    sendSqlStatement(sqlStatement, nullptr, nullptr);

    // then delete the album
    sqlStatement = "DELETE FROM ALBUMS WHERE NAME = '" + albumName + "' AND USER_ID = " + std::to_string(userId) + ";"; // create statement of deleting album
    sendSqlStatement(sqlStatement, nullptr, nullptr); // send the statement to db (executing)
}

// will add picture to album
void DatabaseAccess::addPictureToAlbumByName(const std::string& albumName, Picture& picture)
{
    int lastPictureId; // will store the last picture id + 1 for knowing what should be next

    std::string sqlStatement = "SELECT COALESCE(MAX(ID) + 1, 101) from PICTURES"; // create statement for getting last id + 1 (if no pictures will return 101)
    sendSqlStatement(sqlStatement, callBackGetInt, &lastPictureId); // send sql statement that will return last id + 1 or 101

    picture.setId(lastPictureId); // // set the right id for picture

    sqlStatement = "INSERT INTO PICTURES (ID, NAME, LOCATION, CREATION_DATE, ALBUM_ID) VALUES (" + std::to_string(lastPictureId) + ", '" + picture.getName() + "', '" + picture.getPath() + "', '" + picture.getCreationDate() + "', (SELECT ID FROM ALBUMS WHERE NAME = '" + albumName + "'))"; // create statement of adding picture
    sendSqlStatement(sqlStatement, nullptr, nullptr); // send the statement to db (executing)
}

// will remove picture by name
void DatabaseAccess::removePictureFromAlbumByName(const std::string& albumName, const std::string& pictureName)
{
    // first delete the tags of picture
    std::string sqlStatement = "DELETE FROM TAGS WHERE PICTURE_ID = (SELECT ID FROM PICTURES WHERE NAME = '" + pictureName + "' AND ALBUM_ID = (SELECT ID FROM ALBUMS WHERE NAME = '" + albumName + "'));"; // create statement of deleting tags
    sendSqlStatement(sqlStatement, nullptr, nullptr); // execute statement

    // then delete the picture
    sqlStatement = "DELETE FROM PICTURES WHERE NAME = '" + pictureName + "' AND ALBUM_ID = (SELECT ID FROM ALBUMS WHERE NAME = '" + albumName + "');"; // create statement of deleting piture
    sendSqlStatement(sqlStatement, nullptr, nullptr); // send the statement to db (executing)
}

// will tag user in picture. By userId and picture name given in parameters
void DatabaseAccess::tagUserInPicture(const std::string& albumName, const std::string& pictureName, int userId)
{
    // add tag to db
    std::string sqlStatement = "INSERT INTO TAGS (PICTURE_ID, USER_ID) VALUES ((SELECT ID FROM PICTURES WHERE NAME = '" + pictureName + "' AND ALBUM_ID = (SELECT ID FROM ALBUMS WHERE NAME = '" + albumName + "') ), " + std::to_string(userId) + ");"; // create statement of tagging user
    sendSqlStatement(sqlStatement, nullptr, nullptr); // send the statement to db (executing)
}

// will untag user in picture. By userId and picture name given in parameters
void DatabaseAccess::untagUserInPicture(const std::string& albumName, const std::string& pictureName, int userId)
{
    std::string sqlStatement = "DELETE FROM TAGS WHERE USER_ID = " + std::to_string(userId) + " AND PICTURE_ID = (SELECT ID FROM PICTURES WHERE NAME = '" + pictureName + "' AND ALBUM_ID = (SELECT ID FROM ALBUMS WHERE NAME = '" + albumName + "' ));"; // create statement of untagging user
    sendSqlStatement(sqlStatement, nullptr, nullptr); // send the statement to db (executing)
}

// will create user in db with the given user object
void DatabaseAccess::createUser(User& user)
{
    int lastUserId;// will store last user id + 1 for knowing what should be next

    std::string sqlStatement = "SELECT COALESCE(MAX(ID) + 1, 201) from USERS"; // create statement for getting last id + 1 (if no pictures will return 201)
    sendSqlStatement(sqlStatement, callBackGetInt, &lastUserId); // send sql statement that will return last id + 1 or 201

    user.setId(lastUserId); // set the right id for user

    // the insert user to db
    sqlStatement = "INSERT INTO USERS (ID, NAME) VALUES (" + std::to_string(lastUserId) + ", '" + user.getName() + "');"; // create statement of creating user
    sendSqlStatement(sqlStatement, nullptr, nullptr); // send the statement to db (executing)
}

// will delete user from db with the given user object
void DatabaseAccess::deleteUser(const User& user)
{
    std::list<Album> userAlbumsList; // will store user albums for deleting them first

    // get the albums that should be deleted
    std::string sqlStatement = "SELECT * FROM ALBUMS WHERE USER_ID = " + std::to_string(user.getId()) + ";"; // create statement of deleting albums
    sendSqlStatement(sqlStatement, callBackGetAlbums, &userAlbumsList); // execute statement

    // run on the alvums and delete them
    for (Album album : userAlbumsList)
    {
        deleteAlbum(album.getName(), album.getOwnerId());
    }

    // then delete the user
    sqlStatement = "DELETE FROM USERS WHERE ID = " + std::to_string(user.getId()) + ";"; // create statement of deleting user
    sendSqlStatement(sqlStatement, nullptr, nullptr); // send the statement to db (executing)
}

DatabaseAccess::DatabaseAccess()
{
}

DatabaseAccess::~DatabaseAccess()
{
    close();
}

// this function will return all existing albums in list
const std::list<Album> DatabaseAccess::getAlbums()
{
    std::list<Album> albumsList; // will store the result

    std::string sqlStatement = "SELECT * FROM ALBUMS;"; // create statement of getting albums
    sendSqlStatement(sqlStatement, callBackGetAlbums, &albumsList); // execute statement

    return albumsList;
}

// this function will return all existing albums of a specific user
const std::list<Album> DatabaseAccess::getAlbumsOfUser(const User& user)
{
    std::list<Album> albumsList; // will store the result

    std::string sqlStatement = "SELECT * FROM ALBUMS WHERE USER_ID = " + std::to_string(user.getId()) + ";"; // create statement of getting albums
    sendSqlStatement(sqlStatement, callBackGetAlbums, &albumsList); // execute statement

    return albumsList;
}

// will create album from pattern given in parameters
void DatabaseAccess::createAlbum(const Album& album)
{
    std::list<Album> albumsList = getAlbums(); // get all albums
    
    // check if album with this name already exist, and throw exception if its true
    for (Album tempAlbum : albumsList)
    {
        if (tempAlbum.getName() == album.getName())
        {
            throw MyException("Error, album with this name already exist, try another name");
        }
    }

    // then insert album
    std::string sqlStatement = "INSERT INTO ALBUMS (NAME, CREATION_DATE, USER_ID) VALUES ('" + album.getName() + "', '" + album.getCreationDate() + "', " + std::to_string(album.getOwnerId()) + ");"; // create statement of creating album
    sendSqlStatement(sqlStatement, nullptr, nullptr); // execute statement 
}

/*
input: sqlStatement that should be executed, callback function (may be nullptr), data for output from callback (also may be nullptr)
output: null
function will execute sql statement and return result from callback to the third parameter if second and third parameters are not null
*/
void DatabaseAccess::sendSqlStatement(std::string sqlStatement, int(*callback)(void*, int, char**, char**), void* data)
{
    char* errMessage = nullptr;
    int res = sqlite3_exec(db, sqlStatement.c_str(), callback, data, &errMessage); // execute statement
    if (res != SQLITE_OK)
        throw MyException(errMessage);
}

// will check if albums with given parameters exist
bool DatabaseAccess::doesAlbumExists(const std::string& albumName, int userId)
{
    std::list<Album> albumsList = getAlbums(); // get all albums

    // run on the albums
    for (const auto album : albumsList)
    {
        if (album.getName() == albumName && album.getOwnerId() == userId) // if album with this user id and name exist
        {
            return true; // return true
        }
    }
    return false; // if album wasnt found return false
}

void DatabaseAccess::printAlbums()
{
    std::list<Album> albumsList = getAlbums(); // get all albums

    // print the intro
    std::cout << "Album list:" << std::endl;
    std::cout << "-----------" << std::endl;

    // run on the albums
    for (const auto album : albumsList)
    {
        std::cout << std::setw(5) << "* " << album; // print the album info
    }
}

// function will return list of users
const std::list<User> DatabaseAccess::getUsers()
{
    std::list<User> usersList; // will store the result

    std::string sqlStatement = "SELECT * FROM USERS;"; // create statement of getting users
    sendSqlStatement(sqlStatement, callBackGetUsers, &usersList); // execute statement

    return usersList;
}

// this function will print all users
void DatabaseAccess::printUsers()
{
    std::list<User> usersList = getUsers(); // get all users

    // print intro
    std::cout << "Users list:" << std::endl;
    std::cout << "-----------" << std::endl;

    // run over the list
    for (const auto user : usersList) 
    {
        std::cout << user << std::endl; // print users properties
    }
}

// function will return user by his id
User DatabaseAccess::getUser(int userId)
{
    std::list<User> usersList = getUsers(); // get all users

    // run over the list
    for (const auto user : usersList)
    {
        if (user.getId() == userId) // when we found the right user
        {
            return user; // return him
        }
    }
}

// function will return true if found user with given id and false if not
bool DatabaseAccess::doesUserExists(int userId)
{
    std::list<User> usersList = getUsers(); // get all users

    // run over the list
    for (const auto user : usersList)
    {
        if (user.getId() == userId) // when we found the right user
        {
            return true; // return true
        }
    }
    return false; // return false if user wasnt found
}

// will return the amount of albums owned by user
int DatabaseAccess::countAlbumsOwnedOfUser(const User& user)
{
    return getAlbumsOfUser(user).size();
}

// will return the amount of albums that user tagged in
int DatabaseAccess::countAlbumsTaggedOfUser(const User& user)
{
    int result; // will store the amount of albums
    std::string sqlStatement = "SELECT COUNT (DISTINCT ALBUM_ID) FROM TAGS INNER JOIN PICTURES ON TAGS.PICTURE_ID = PICTURES.ID WHERE TAGS.USER_ID = " + std::to_string(user.getId()) + ";"; // create statement of getting the amount
    sendSqlStatement(sqlStatement, callBackGetInt, &result); // execute statement
    return result;
}

// will return the amount of user tags
int DatabaseAccess::countTagsOfUser(const User& user)
{
    std::list<std::pair<int, int>> tagsList; // will store the tags (first = user_id, second = picture_id)
    std::string sqlStatement = "SELECT * FROM TAGS WHERE USER_ID = " + std::to_string(user.getId()) + ";"; // create getting tags sql statement
    sendSqlStatement(sqlStatement, callbackGetTags, &tagsList); // execute statement

    return tagsList.size(); // return the result
}

// will return average amount of tags in tagged albums
float DatabaseAccess::averageTagsPerAlbumOfUser(const User& user)
{
    int albumsTaggedCount = countAlbumsTaggedOfUser(user); // get the amount of albums that user tagged in

    if (0 == albumsTaggedCount)  // if the amount is 0
    {
        return 0; // return 0
    }

    return static_cast<float>(countTagsOfUser(user)) / albumsTaggedCount;
}

// will return the most tagged user
User DatabaseAccess::getTopTaggedUser()
{
    std::list<User> usersList = getUsers(); // get all users
    User tempUser(0, ""); // will be the temp user for each loop
    int topTagsAmount = 0; // will store the amount of top tags for comparing

    for (const auto user : usersList) // run on the users
    {
        if (countTagsOfUser(user) > topTagsAmount) // if the mount of tags of curr user is bigger then top amount
        {
            topTagsAmount = countTagsOfUser(user); // swap amounts
            tempUser = user; // put the user into temp user
        }
    }
    if (topTagsAmount > 0) 
    {
        return tempUser;
    }
    else
    {
        throw MyException("No tags found\n"); // throw exception if 0 tags was found
    }
}

// will return picture with the most tagss
Picture DatabaseAccess::getTopTaggedPicture()
{
    int theMostTaggedPictureId; 
    std::list<Picture> theMostTaggedPicture; 

    // get the most tagged picture id
    std::string sqlStatement = "SELECT PICTURE_ID FROM TAGS GROUP BY PICTURE_ID ORDER BY COUNT(*) DESC LIMIT 1;";
    sendSqlStatement(sqlStatement, callBackGetInt, &theMostTaggedPictureId);

    // get the most tagged picture 
    sqlStatement = "SELECT * FROM PICTURES WHERE ID = " + std::to_string(theMostTaggedPictureId) + ";";
    sendSqlStatement(sqlStatement, callBackGetPictures, &theMostTaggedPicture);

    return theMostTaggedPicture.front(); // return .front because list will have only one element
}

// will get list of pictures
const std::list<Picture> DatabaseAccess::getPictures()
{
    std::list<Picture> picturesList; // will store the result

    std::string sqlStatement = "SELECT * FROM PICTURES;"; // create statement of getting pictures
    sendSqlStatement(sqlStatement, callBackGetPictures, &picturesList);
     
    return picturesList;
}

// get pictures that user tagged in
std::list<Picture> DatabaseAccess::getTaggedPicturesOfUser(const User& user)
{
    std::list<Picture> picturesList; // will store the result
    std::string sqlStatement = "SELECT DISTINCT PICTURES.ID, PICTURES.NAME, PICTURES.LOCATION, PICTURES.CREATION_DATE, PICTURES.ALBUM_ID FROM PICTURES INNER JOIN TAGS ON TAGS.PICTURE_ID = PICTURES.ID WHERE TAGS.USER_ID = " + std::to_string(user.getId()) + ";"; // create statement of getting pictures
    sendSqlStatement(sqlStatement, callBackGetPictures, &picturesList); // execute statement
    return picturesList;
}

// will update all album information and return it
Album DatabaseAccess::openAlbum(const std::string& albumName)
{
    std::list<Picture> picturesList;
    std::list<std::pair<int, int>> tagsList;

    for (Album album : getAlbums()) {
        if (albumName == album.getName()) {
            std::string sqlStatement = "SELECT * FROM PICTURES WHERE ALBUM_ID = (SELECT ID FROM ALBUMS WHERE NAME = '" + albumName + "');";
            sendSqlStatement(sqlStatement, callBackGetPictures, &picturesList);

            for (Picture picture : picturesList)
            {
                tagsList.clear();
                sqlStatement = "SELECT * FROM TAGS WHERE PICTURE_ID = " + std::to_string(picture.getId()) + ";";

                std::cout << sqlStatement << std::endl;

                sendSqlStatement(sqlStatement, callbackGetTags, &tagsList);

                for (std::pair<int, int> tempTag : tagsList)
                {
                    picture.tagUser(tempTag.first);
                }

                album.addPicture(picture);
            }

            return album;
        }
    }
    throw MyException("No album with name " + albumName + " exists");
}
