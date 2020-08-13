from pymongo import MongoClient
import datetime
import time
import playsound

date = datetime.date.today()

cumulative_collec = f"cumulative_{date}"
client_strategy_collec = f"Client_Strategy_Status"
all_list_algo_collec = f"algo"
all_list_client_collec = f"client"
error_collec = f"position_mismatch_error_{date}"
connection = MongoClient('localhost', 27017)

try:
    cumulative_db = connection['Cumulative_symphonyorder']
    client_strategy_db = connection['Client_Strategy_Status']
    all_list_db = connection['all_list']

except Exception:
    print("ERROR: Mongo Connection Error123")


try:
    error_db = connection['position_mismatch_error']
    error_db[error_collec].drop()
    print('error Collec Deleted')
except:
    pass

algo = all_list_db[all_list_algo_collec].distinct("key")
algo.remove("All")
print(algo)

client = all_list_db[all_list_client_collec].distinct("client")
client.remove("All")
client.remove("D7730003")
# print(client)

# row=client_strategy_db[client_strategy_collec].find({"Start_Stop":"START"})
global lastAlarmTime
lastAlarmTime = time.time()


def save_error(value):
    # print(value)
    global lastAlarmTime
    try:
        client = MongoClient()
        db = client['position_mismatch_error']
        collec = f"position_mismatch_error_{date}"
        db.create_collection(collec)
        print(f"Created New Collection '{collec}'")
        db[collec].insert(value)
        # print(post)
        # print("run")
        if time.time() - lastAlarmTime > 60:
            playsound.playsound("Error-sound.mp3")
            lastAlarmTime = time.time()

    except Exception:
        match_val = db[collec].find_one({"client id":value["client id"],"symbol":value["symbol"],"algoname":value["algoname"]})
        # print(match_val)
        if match_val:
            if(match_val["quantity"]!=value["quantity"]):
                # print("quantity not match")
                db[collec].delete_one(match_val)
                db[collec].insert(value)
                # db[collec].update({'_id': match_val['_id']}, {"$set": {"quantity":value["quantity"]}})
            #     print(match_val)
        else:
            db[collec].insert(value)
            # print("not run")
            if time.time() - lastAlarmTime > 60:
                playsound.playsound("Error-sound.mp3")
                lastAlarmTime = time.time()


base_id = "D7730003"


def define_error(i, word, q, state, symbol, algo):
    error = f"{i} {word} {abs(int(q))} {state} {symbol} {algo}"
    error_dict = {"client id":i,"condition":word,"quantity":int(q),"state":state,"symbol":symbol,"algoname":algo,"error reason": error}
    save_error(error_dict)


def position_db():

    for j in algo:
        match = cumulative_db[cumulative_collec].find(
            {"$and": [{"algoName": j}, {"clientID": base_id}]})
        # print(match)
        if match:
            for sym in match:
                # print(sym)
                symbol = sym["symbol"]
                # print(symbol)
                quantity = sym["quantity"]
                new_match = client_strategy_db[client_strategy_collec].find_one(
                    {"$and": [{"algoname": j}, {"ClientID": base_id}]})
                # print(new_match)
                if new_match["Start_Stop"] == "START":
                    multiple = new_match["quantity_multiple"]
                    unitq = quantity//int(multiple)
                    # print(unitq,quantity,multiple)
                    if(symbol[:5] == "NIFTY"):
                        if((unitq % 75) != 0):
                            #     pass
                            # else:
                            unitq = unitq-(unitq % 75)
                            # print(unitq)
                    elif(symbol[:9] == "BANKNIFTY"):
                        if((unitq % 20) != 0):
                            #     pass
                            # else:
                            unitq = unitq-(unitq % 20)
                    # print(unitq)
                else:
                    if(quantity > 0):
                        # print("run")
                        define_error(base_id, "remaining", str(
                            quantity), "quantity BUY in STOPPED ", symbol, sym["algoName"])
                        # error = base_id+" remaining quantity " + str(quantity)+" "+symbol+" " + sym["algoName"]+" is STOPPED"
                    elif(quantity<0):
                        define_error(base_id, "remaining", str(
                            quantity), "quantity SELL in STOPPED ", symbol, sym["algoName"])

                for i in client:
                    new_client = cumulative_db[cumulative_collec].find_one(
                        {"$and": [{"algoName": sym["algoName"]}, {"clientID": i}, {"symbol": symbol}]})
                    new_matchID = client_strategy_db[client_strategy_collec].find_one(
                        {"$and": [{"algoname": sym["algoName"]}, {"ClientID": i}]})
                    if new_matchID:
                        if new_matchID["Start_Stop"] == "START":
                            new_multiple = new_matchID["quantity_multiple"]
                            required_q = unitq*int(new_multiple)
                            # print("required_q ", required_q, " unitq ", unitq,
                            # " new_multiple ", new_multiple, " new_quantity ", new_quantity)
                            if new_client:

                                new_quantity = new_client["quantity"]
                            
                                if(required_q != new_quantity):
                                    extraq = new_quantity-required_q
                                    # print(extraq)
                                    if quantity < 0:
                                        extraq = (-extraq)
                                    if(extraq > 0):
                                        if(quantity > 0):
                                            define_error(i, "Extra", str(
                                                extraq), "quantity BUY in", symbol, sym["algoName"])
                                        elif(quantity == 0):
                                            define_error(i, "Extra", str(
                                                extraq), "quantity BUY in", symbol, sym["algoName"])
                                        else:
                                            define_error(i, "Extra", str(
                                                extraq), "quantity SELL in", symbol, sym["algoName"])

                                    else:
                                        if(quantity > 0):
                                            define_error(i, "Missing", str(
                                                extraq), "quantity BUY in", symbol, sym["algoName"])
                                        elif(quantity == 0):
                                            define_error(i, "Extra", str(
                                                extraq), "quantity SELL in", symbol, sym["algoName"])
                                        else:
                                            # define_error(i,"Missing",str(extraq)),"quantity BUY in",symbol,sym["algoName"])
                                            define_error(i, "Missing", str(
                                                extraq), "quantity SELL in", symbol, sym["algoName"])
                            else:
                                if(required_q > 0):
                                    # print(required_q)
                                    define_error(i, "Missing", str(
                                        required_q), "quantity BUY in", symbol, sym["algoName"])
                                elif(required_q < 0):
                                    # print(required_q)
                                    define_error(i, "Missing", str(
                                        required_q), "quantity SELL in", symbol, sym["algoName"])

                        elif new_matchID["Start_Stop"] == "STOP":
                            if new_client:
                                new_quantity = new_client["quantity"]
                                if(new_quantity > 0):
                                    # print("i run")
                                    print(new_quantity)
                                    define_error(i, "remaining", str(
                                        new_quantity), "quantity BUY in STOPPED ", symbol, sym["algoName"])
                                    # error = i+" remaining quantity " + str(quantity)+" "+symbol+" " + sym["algoName"] + "is STOPPED"
                                elif(new_quantity<0):
                                    define_error(i, "remaining", str(
                                        new_quantity), "quantity SELL in STOPPED ", symbol, sym["algoName"])

                    

orderResponseDbName = f'final_response'
orderResponseDb = connection[orderResponseDbName]
responseCollecName = f'final_response_{date}'
resCount = orderResponseDb[responseCollecName].estimated_document_count()

while True:
    currentCount = orderResponseDb[responseCollecName].estimated_document_count()
    if currentCount!=resCount:
        time.sleep(3)
        print('Running now: ', datetime.datetime.now().time())
        position_db()
        resCount = currentCount
    else:
        time.sleep(1)    
