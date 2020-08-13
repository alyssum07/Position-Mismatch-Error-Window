from tkinter import *
import tkinter.messagebox
from pymongo import MongoClient
import datetime
import time
# import playsound

date = datetime.date.today()
raw_collec = f'final_response_{date}'

connection = MongoClient('localhost', 27017)

try:
    db = connection['final_response']
except Exception:
    print("ERROR: Mongo Connection Error123")
try:

    error_db = connection['position_mismatch_error']
    error_collec=f"position_mismatch_error_{date}"
except:
    print("ERROR: Mongo Connection Error123")

root = Tk()
root.title("Position MisMatch Monitoring ")
root.geometry("1200x1200")

MainFrame=Frame(root)
MainFrame.grid()

TitleFrame = Frame(MainFrame,padx=300,pady=8,bg="#0f9d58",bd=2)        
TitleFrame.pack(side=TOP)

ButtonFrame = Frame(MainFrame,width=600,height=70,padx=110,pady=10)        
ButtonFrame.pack(side=BOTTOM)
lblframe = Frame(MainFrame,width=1000,height=500,padx=30,pady=15)
lblframe.pack(side=BOTTOM)

labellist = Listbox(lblframe,width=100,height=20, font=('arial', 15, 'bold'),bd=5)

labellist.insert(END,"")
labellist.grid(row=0,column=0)



def db_update(final_match,match,new_post):
    if final_match!="None":
        error_msg=f"cliend id : {match['client id']} \nalgoname : {final_match['algoname']} \nsymbol : {final_match['symbol']} \nnew quantity : {abs(match['quantity'])} \nbuy_sell : {new_post['buy_sell']}"
        msg=tkinter.messagebox.askyesno("Update quantity Info",error_msg)
        if msg>0:
            db[raw_collec].insert_one(new_post)
            return "deleted"
    else:
        tkinter.messagebox.showinfo("client Info","D7730003 is not found")

def savedata(final_match,match,orderside):
    if final_match!="None":
        timestamp = str(datetime.datetime.now().time())
        post={
            "quantity": abs(int(match["quantity"])),
            "algoname": final_match["algoname"],
            "symbol": final_match["symbol"],
            "exchangeInstrumentID": final_match["exchangeInstrumentID"],
            "exchangeSegment": final_match["exchangeSegment"],
            "buy_sell": orderside,
            "time_stamp": timestamp,
            "productType": final_match["productType"],
            "orderStatus": final_match["orderStatus"],
            "cancelrejectreason": final_match["cancelrejectreason"],
            "OrderAverageTradedPrice": final_match["OrderAverageTradedPrice"],
            "clientID": match["client id"]
        }
        return post
    else:
        return "None"
def update(match):
    final_match=db[raw_collec].find_one({"$and" : [{"algoname":match["algoname"]},{"symbol":match["symbol"]},{"clientID":"D7730003"}] })
    # print(final_match)
    if final_match:
        # print(final_match)
        if ((match["condition"]=="Extra") or (match["condition"]=="remaining")):
            if match["state"][9:12] == "BUY":
                order="SELL"
               
            elif match["state"][9:13] == "SELL":
                order="BUY"
               
        elif match["condition"]=="Missing":
            if match["state"][9:12] == "BUY":
                order="BUY"
                
            elif match["state"][9:13] == "SELL":
                order="SELL"
    
        return final_match,match,order
    else:
        return "None","None","None"

def resolve():
    try:
        labellist.bind('<<ListboxSelect>>')
        searchlbl = labellist.curselection()
        error_val= labellist.get(searchlbl)
        global running
        running="false"
        # print(running)
        match=error_db[error_collec].find_one({"error reason": error_val})
        # print(match)
        update_match,match1,order=update(match)
        post=savedata(update_match,match1,order)
        e_msg=db_update(update_match,match1,post)
        
        if e_msg=="deleted":
            # print("done")
            labellist.delete(searchlbl)
            error_db[error_collec].delete_one({"error reason": error_val})
            running="true"
        else:
            running="true"

    except Exception as e:
        print(e)

def delete():
    try:
        labellist.bind('<<ListboxSelect>>')
        searchlbl = labellist.curselection()
        error_val= labellist.get(searchlbl)
        global running
        running="false"
        msg1=tkinter.messagebox.askyesno("delete msg","You want to delete ?")
        if msg1>0:
            labellist.delete(searchlbl)
            print(error_val)
            error_db[error_collec].delete_one({"error reason": error_val})
            running="true"
        else:
            running="true"
    except Exception as e:
        print(e)

def print_err():
    label=Label(lblframe)
    string =error_db[error_collec].find()
    for i in string:
        # print(i)
        label['text'] = i["error reason"]
        # print(label['text'])
        labellist.insert(0,(label['text']))
        labellist.grid(row=0,column=0)
        

def new_window():
    # print(running)
    global err_match
    if running=="true":
        new_err_match=error_db[error_collec].distinct("_id")
        
        if(err_match!=new_err_match):
            labellist.delete('0','end')
            print("deleted enteries")
            err_match=new_err_match
            print_err()

    root.after(3000,new_window)
    # root.mainloop()

lblTitle = Label(TitleFrame, bd=2,font=('arial',20,'bold'), text="Position MisMatch Monitoring",bg="#0f9d58",fg="white")
lblTitle.grid(sticky=W)

btnResolve = Button(ButtonFrame, text="Resolve",font=('arial',20,'bold'),height=1,width=10,bd=5,
                activeforeground="white",activebackground="blue", command=resolve,state=DISABLED)
btnResolve.grid(row=0,column=0)

btnDelete = Button(ButtonFrame, text="Delete",font=('arial',20,'bold'),height=1,width=10,bd=5,
                activeforeground="white",activebackground="blue", command=delete)
btnDelete.grid(row=0,column=1)

running="true"
err_match=error_db[error_collec].distinct("_id")
print_err()
new_window()
root.mainloop()
