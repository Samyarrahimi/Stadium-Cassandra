from random import randrange

from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

from cassandra.cluster import Cluster
from flask import Flask, render_template, make_response, redirect, url_for, session
from flask_restful import Api, Resource, reqparse

app = Flask(__name__, template_folder="templates")
api = Api(app)

post_parser = reqparse.RequestParser()
post_parser.add_argument("selected_class", type=str)
post_parser.add_argument("username", type=str)

cluster = Cluster(['127.0.0.1'], port=9042)
sess = cluster.connect()

# sess.execute("CREATE KEYSPACE stadium WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};")
sess.execute("USE stadium")

sess.execute(
    "CREATE TABLE stadium.tickets (class text,seat_no int,username text,reserve_time timestamp ,finalized int, PRIMARY KEY (class, seat_no));")
# sess.execute("CREATE TABLE stadium.remainings (A_rem int,B_rem int,C_rem int,D_rem int, id int PRIMARY KEY );")
# sess.execute("CREATE TABLE stadium.cleanings (id int PRIMARY KEY,clean_time timestamp);")

sess.execute("INSERT INTO stadium.cleanings (id,clean_time) VALUES (1,toTimeStamp(now()))")

sess.execute("INSERT INTO stadium.remainings (A_rem, B_rem, C_rem, D_rem, id) VALUES (5000 , 5000, 5000, 5000, 1);")

ticket_count = 5000

now = datetime.now()
c_time = now.strftime("%H:%M:%S")

sess.execute("INSERT INTO stadium.tickets (class, seat_no, username, reserve_time, finalized)"
             " VALUES ('A' , 0, 'tmp', toTimeStamp(now()), 1);")
sess.execute("INSERT INTO stadium.tickets (class, seat_no, username, reserve_time, finalized)"
             " VALUES ('B' , 0, 'tmp', toTimeStamp(now()) , 1);")
sess.execute("INSERT INTO stadium.tickets (class, seat_no, username, reserve_time, finalized)"
             " VALUES ('C' , 0, 'tmp', toTimeStamp(now()) , 1);")
sess.execute("INSERT INTO stadium.tickets (class, seat_no, username, reserve_time, finalized)"
             " VALUES ('D' , 0, 'tmp', toTimeStamp(now()) , 1);")


def get_max_id_cleanings():
    return sess.execute("SELECT MAX(id) FROM cleanings;").one()[0]


def clean_invalid_reservations():
    minutes_to_wait = 1
    print("removing invalid reservations from DB")

    max_id = get_max_id_cleanings()
    max_id += 1
    query = "INSERT INTO stadium.cleanings (id,clean_time) VALUES (1,toTimeStamp(now()))"
    sess.execute(query)

    query = "SELECT clean_time from cleanings WHERE id=1"
    now = sess.execute(query).one()

    n = now[0].strftime("%H:%M:%S")

    query = "SELECT class,seat_no,finalized,reserve_time FROM tickets WHERE finalized=0 ALLOW FILTERING"
    rows = sess.execute(query)

    my_hour = int(n[:2])
    my_minute = int(n[3:5])
    if my_minute - minutes_to_wait < 0:
        my_minute = 59
        my_hour -= 1

    classes = []
    seat_nos = []
    for row in rows:
        # should be removed
        if row[3].hour < my_hour or row[3].minute < my_minute:
            classes.append(row[0])
            seat_nos.append(row[1])

    query = "DELETE FROM tickets WHERE class=%s and seat_no=%s IF EXISTS;"
    for i in range(len(classes)):
        sess.execute(query, (classes[i], seat_nos[i]))
        update_class_rem(classes[i], min(ticket_count, get_class_rem(classes[i]) + 1))


scheduler = BackgroundScheduler()
time_passed_from_reservation = 1
scheduler.add_job(func=clean_invalid_reservations, trigger="interval", seconds=40)
scheduler.start()


def get_last_seat(selected_class):
    rows = sess.execute(f"SELECT seat_no FROM tickets WHERE class = '{selected_class}';")
    seats = []
    for row in rows:
        seats.append(row[0])

    prev_val = seats[0]
    val = 0
    for val in seats[1:]:
        if prev_val + 1 < val:
            return prev_val
        prev_val = val
    return val


def get_class_rem(selected_class):
    selected_class += "_rem"
    return sess.execute("SELECT " + selected_class + " FROM stadium.remainings").one()[0]


def update_class_rem(selected_class, value):
    selected_class += "_rem"
    return sess.execute("UPDATE stadium.remainings SET " + str(selected_class) + " = " + str(value) + " WHERE id = 1")


def finalize_ticket(selected_class, seat_no):
    query = "UPDATE stadium.tickets SET finalized=1 WHERE class=%s and seat_no=%s"
    sess.execute(query, (selected_class, seat_no))


class index_handler(Resource):
    def get(self):
        return make_response(render_template("index.html"
                                             , A_rem=get_class_rem("A")
                                             , B_rem=get_class_rem("B")
                                             , C_rem=get_class_rem("C")
                                             , D_rem=get_class_rem("D")))


class post_redirect_get_index(Resource):
    def post(self):
        params = post_parser.parse_args()
        selected_class = params["selected_class"]

        seat_no = session["seat_no"]

        query = "DELETE FROM tickets WHERE class=%s and seat_no=%s IF EXISTS;"
        sess.execute(query, (selected_class, seat_no))

        update_class_rem(selected_class, min(ticket_count, get_class_rem(selected_class) + 1))

        return redirect(url_for("index_handler"))


class post_redirect_get_payment(Resource):
    def post(self):
        params = post_parser.parse_args()
        selected_class = params["selected_class"]

        username = params["username"]

        seat_no = get_last_seat(selected_class) + 1

        # all tickets are sold out
        if seat_no > ticket_count:
            session[selected_class + "_done"] = 1
            return redirect(url_for("payment_show_handler"))

        query = "INSERT INTO tickets (class, seat_no, username, reserve_time, finalized) VALUES ( %s , %s , %s , toTimeStamp(now()) , %s);"
        sess.execute(query, (selected_class, seat_no, username, 0))

        update_class_rem(selected_class, min(ticket_count, get_class_rem(selected_class) - 1))

        session["selected_class"] = selected_class
        session["seat_no"] = seat_no

        return redirect(url_for("payment_show_handler"))


class payment_show_handler(Resource):
    def get(self):
        selected_class = session["selected_class"]
        return make_response(render_template("payment.html", selected_class=selected_class))


class payment_successful_handler(Resource):
    def post(self):
        params = post_parser.parse_args()
        selected_class = params["selected_class"]

        p = selected_class + "_done"
        if p not in session:
            info = "You bought a ticket from class " + selected_class

            seat_no = session["seat_no"]

            finalize_ticket(selected_class, seat_no)

        else:
            info = "Sorry! all tickets in class " + selected_class + " are sold."

        return make_response(render_template("paymentdone.html", info=info
                                             , A_rem=get_class_rem("A")
                                             , B_rem=get_class_rem("B")
                                             , C_rem=get_class_rem("C")
                                             , D_rem=get_class_rem("D")))


api.add_resource(index_handler, "/")
api.add_resource(post_redirect_get_index, "/indexprg")
api.add_resource(post_redirect_get_payment, "/paymentprg")
api.add_resource(payment_show_handler, "/payment")
api.add_resource(payment_successful_handler, "/paymentdone")

if __name__ == "__main__":
    key_num = randrange(10000)
    app.secret_key = "stadium" + str(key_num)
    app.run(host="0.0.0.0", debug=False)
    session.clear()
