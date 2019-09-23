# ----- CONFIGURE YOUR EDITOR TO USE 4 SPACES PER TAB ----- #
import sys, os
sys.path.append(os.path.join(os.path.split(os.path.abspath(__file__))[0], 'lib'))
import pymysql as db
import settings
import operator


def connection():
    ''' User this function to create your connections '''
    con = db.connect(
        settings.mysql_host,
        settings.mysql_user,
        settings.mysql_passwd,
        settings.mysql_schema)

    return con


def classify(topn):

    # Create a new connection
    con = connection()
    # Create a cursor on the connection
    cur = con.cursor()

    def findCategory(categories):
        hkat = categories[0]
        for cat in categories:
            if cat[3] > hkat[3]:
                hkat = cat
        return hkat

    sqlQuery = """
        SELECT
            title, summary
        FROM
            articles
        WHERE
            NOT id IN (
                SELECT
                    articles_id
                FROM
                    article_has_class
            )
    ;"""
    categoryQuery = """
        SELECT
            DISTINCT class, subclass
        FROM
            classes
        LIMIT %d
    ;""" % ( int(topn))

    cur.execute(sqlQuery)
    data = cur.fetchall()
    cur.execute(categoryQuery)
    categoriesData = cur.fetchall()

    katigories = []
    for row in categoriesData:
        info = [
            row[0].split(), # class
            row[1].split(), # subclass
            1,              # weight
            0               # weight count
        ]
        katigories.append(info)

    articles = []
    for row in data:
        array = [
            row[0],         # title
            row[1].split(), # summary
            katigories      # array of classes, subclasses, weight and the weight count
        ]
        articles.append(array)

    for arthro in articles:
        print(arthro[0])
        for word in arthro[1]:
            for katigoria in arthro[2]:
                for classW in katigoria[0]:
                    if word.lower() == classW.lower():
                        katigoria[3] = katigoria[3] + katigoria[2]
                for subclassW in katigoria[1]:
                    if word.lower() == subclassW.lower():
                        katigoria[3] = katigoria[3] + katigoria[2]

    # for article in articles:
    #     print("Article's name: ", article[0])
    #     for katigoria in article[2]:
    #         print("Class = ", " ".join(katigoria[0]), "Subclass = ", " ".join(katigoria[1]), " Wcount: ", katigoria[3])
    #     print("\n")

    apotelesmata = [("title", "class", "subclass", "weightsum"), ]
    for arthro in articles:
        Thecat = findCategory(arthro[2])
        info = (
            arthro[0],
            " ".join(Thecat[0]),
            " ".join(Thecat[1]),
            Thecat[3]
        )
        apotelesmata.append(info)

    cur.close()
    con.close()

    return apotelesmata

 
def updateweight(class1, subclass, weight):

   # Create a new connection
    con = connection()
    # Create a cursor on the connection
    cur = con.cursor()

    sqlQuery = """
        UPDATE
            classes
        SET
            classes.weight = classes.weight - (classes.weight - %d) / 2
        WHERE
            classes.class = "%s"
            AND classes.subclass = "%s"
            AND classes.weight > %d
    ;""" % ( int(weight), class1, subclass, int(weight))

    try:
        cur.execute(sqlQuery)
        con.commit()
        data = (("ok", ), )
    except:
        con.rollback()
        data = (("error", ), )

    cur.close()
    con.close()

    apotel = [("apotel", ), ]
    for row in data:
        apotel.append(row)

    return apotel

def selectTopNClasses(fromdate, todate,n):

    # Create a new connection
    con = connection()
    # Create a cursor on the connection
    cur = con.cursor()

    sqlQuery = """
        SELECT
            article_has_class.class, article_has_class.subclass, COUNT(DISTINCT article_has_class.articles_id)
        FROM
            article_has_class
        WHERE
            article_has_class.articles_id IN (
                SELECT
                    articles.id
                FROM
                    articles, article_has_class
                WHERE
                    articles.id = article_has_class.articles_id
                    AND articles.date > "%s"
                    AND articles.date < "%s"
            )
        GROUP BY
            article_has_class.class, article_has_class.subclass
        ORDER BY
            COUNT(DISTINCT article_has_class.articles_id) DESC
        LIMIT %d
    ;""" % ( fromdate, todate, int(n))

    cur.execute(sqlQuery)
    data = cur.fetchall()
    cur.close()
    con.close()

    apotel = [("class", "subclass", "count"), ]
    for row in data:
            apotel.append(row)

    return apotel

def countArticles(class1, subclass):

    # Create a new connection
    con = connection()
    # Create a cursor on the connection
    cur = con.cursor()

    sqlQuery = """
        SELECT
            COUNT( DISTINCT article_has_class.articles_id)
        FROM
            article_has_class
        WHERE
            article_has_class.articles_id IN (
            SELECT
                article_has_class.articles_id
            FROM
                article_has_class
            WHERE
                article_has_class.class = "%s"
                AND article_has_class.subclass = "%s"
        )
    ;""" % ( class1, subclass)

    cur.execute(sqlQuery)
    dedomena = cur.fetchall()
    cur.close()
    con.close()

    apotel = [("count", ), ]
    for row in dedomena:
        apotel.append(row)

    return apotel

def findSimilarArticles(articleId,n):

    # Create a new connection
    con = connection()
    # Create a cursor on the connection
    cur = con.cursor()

    def Jaccard(theArticle, otherArticle):
        same = 0
        diafora = 0

        for leksi in theArticle:
            for otherWord in otherArticle:
                if leksi.lower() == otherWord.lower():
                    same += 1
                else:
                    diafora += 1

        return same / (same + diafora)

    theArticleQuery = """
        SELECT
            summary
        FROM
            articles
        WHERE
            id = %d
    ;""" % ( int(articleId))
    articlesQuery = """
        SELECT
            id, summary
        FROM
            articles
        WHERE
            NOT id = %d
    ;""" % ( int(articleId))

    cur.execute(theArticleQuery)
    theArticle = cur.fetchall()
    cur.execute(articlesQuery)
    articles = cur.fetchall()

    values = []
    for arthra in articles:
        array = [
            arthra[0],
            Jaccard(theArticle[0][0].split(), arthra[1].split())
        ]
        values.append(array)

    # print(values)
    values.sort(key=operator.itemgetter(1), reverse=True)
    # sorted(values, key=lambda x: x[1])

    for value in values:
        print("id:", value[0], ", value is:", value[1], "\n")

    apotel = [("articleid",), ]
    for i in range(int(n)):
        array = (
            values[i][0],  
        )
        apotel.append(array)

    return apotel