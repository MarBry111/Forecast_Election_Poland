import numpy as np
import pandas as pd
import os
from poll_data import party_in_region, region_in_party
import pickle
from tqdm import tqdm

import multiprocessing as mp

fn = 'model/model1/arr.txt'
step = 0.1

def listener(q, fn):
    '''listens for messages on the q, writes to file. '''

    with open(fn, 'w') as f:
        while 1:
            m = q.get()
            if m == 'kill':
                f.write('killed')
                break
            f.write(str(m) + '\n')
            f.flush()

def worker_changed(arg, q):
    '''stupidly simulates long running process'''
    '''
    s = 'this is a test'
    txt = s + str(arg)
    for i in range(200000):
        txt += s 
    with open(fn, 'rb') as f:
        size = len(f.read())
    res = 'Process' + str(arg), str(size), done
    '''
    res = str(arg)+' '
    q.put(res)
    return res

def save_file(func, X, Y, neigh_ndx, fn, iter=1000):
    #must use Manager queue here, or will not work
    manager = mp.Manager()
    q = manager.Queue()    
    pool = mp.Pool(mp.cpu_count() + 2)

    #put listener to work first
    watcher = pool.apply_async(listener, (q, fn,))

    #fire off workers
    jobs = []
    with tqdm(total=iter) as pbar:
        for i in range(iter):
            job = pool.apply_async(func, (X, Y, i, q, neigh_ndx))
            jobs.append(job)

    # collect results from the workers through the pool result queue
        for job in jobs: 
            job.get()
            pbar.update()

    #now we are done, kill the listener
    q.put('kill')
    pool.close()
    pool.join()

def prepare_data(pool_bool = True):
    #============= Prepare data ===================
    # Stat data
    path = 'dane_years/'
    files = list(filter(lambda x: os.path.isfile(path+x), os.listdir(path)))
    files.sort()

    stat_list = [(lambda x: pd.read_csv(path+x,index_col=0, header=0))(f) for f in files[:-1]]

    for yi in range(len(stat_list)):
        y = files[yi].split('.')[0]
        c = stat_list[yi].columns
        c = [y+'-'+ci for ci in c]
        # c = [y[2:]+'-'+str(ci) for ci in range(len(c))]
        stat_list[yi].columns = c

    # Poll data
    pool_data_middle = pd.read_csv('dane_years/pools_data/percent_votes.csv', index_col=0).iloc[:,:-1]
    pool_data_middle = pool_data_middle.divide(pool_data_middle.sum(1),0)


    # Voting data
    path = 'wyniki_wyborow/Simple/'
    files = list(filter(lambda x: os.path.isfile(path+x), os.listdir(path)))
    files.sort()

    vote_list = [(lambda x: pd.read_csv(path+x,index_col=0, header=0))(f) for f in files[:]]
    vote_list[0] = vote_list[0].iloc[1:,:]

    vote_list[0]['jednostka'] = [j.upper() for j in vote_list[0]['jednostka']]
    vote_list[0] = vote_list[0].sort_values(['jednostka'])
    vote_list[0].columns = ['województwo'] + vote_list[0].columns.values.tolist()[1:] 

    vote_list[0] = vote_list[0].set_index('województwo')

    with open('wojew_neighbours.pkl', 'rb') as f:
        neighbours = pickle.load(f)

    ## Use 2 approaches to estimate date from years without elections
    par_in_reg_list = [vote_list[0].iloc[:,:-1]]
    #region_in_party(df_vote, df_poll)
    reg_in_par_list = [vote_list[0].iloc[:,:-1]]
    for pool in pool_data_middle[1:].iterrows():
        if int(pool[0]) < 2005: df_vote = vote_list[0]
        elif int(pool[0]) < 2007: df_vote = vote_list[1]
        elif int(pool[0]) < 2011: df_vote = vote_list[2]
        elif int(pool[0]) < 2015: df_vote = vote_list[3]
        elif int(pool[0]) < 2019: df_vote = vote_list[4]
        else: df_vote = vote_list[5]
        
        par_in_reg_list.append(party_in_region(df_vote.iloc[:,:-1], pool[1]))
        reg_in_par_list.append(region_in_party(df_vote.iloc[:,:-1], pool[1]))

    for vl, i in zip(vote_list.copy(),[0,4,6,10,14,18]):    
        par_in_reg_list[i] = vl.iloc[:,:-1]
        reg_in_par_list[i] = vl.iloc[:,:-1]

    pool_d = par_in_reg_list if (pool_bool) else reg_in_par_list 

    return pool_data_middle, vote_list, neighbours, pool_d

def prepare_input_data(pool_data_middle,vote_list,neighbours,pool_d):
    X = []
    # iterate over years [from 2002 - 2019]
    for y in range(pool_data_middle.shape[0]-1):
        # iterate over districts
        tmp_x = []
        for d in range(vote_list[0].shape[0]):
            # 1. last election: Blue, Red, Gray
            #    Blue/All
            # 2. neighbours
            # 3. one (1)
            lo = pool_d[y].iloc[d,:]
            neigh = neighbours[lo.name.lower()]
            avg_n = [pool_d[y].loc[n.upper()][0]/pool_d[y].loc[n.upper()].sum() for n in neigh]
            avg_n = sum(avg_n)/len(neigh)
            tmp_x.append([lo[0]/lo.sum(), avg_n, 1])
        X.append(tmp_x)

    X = np.array(X)

    Y = []
    for y in range(1,pool_data_middle.shape[0]):
        # iterate over districts
        tmp_y = []
        for d in range(vote_list[0].shape[0]):
            # 1. last election: Blue, Red, Gray
            #    Blue/All
            # 2. neighbours
            # 3. one (1)
            lo = pool_d[y].iloc[d,:]
            tmp_y.append([lo[0]/lo.sum()])
        Y.append(tmp_y)     

    Y = np.array(Y)

    return X, Y

def model_percent(a,x):
    '''
    INPUT:
    a - vector of weights 16x3
    x - vector of input data 18x16x3
    OUTPUT:
    y - predicted value in (0,1)
    '''
    d0 = x.shape[0] if (len(x.shape) == 3) else 1
    
    a = np.repeat(a, d0, 0)
    x = x.reshape(-1, 3)
    #return 1 / (1+np.exp(-np.sum(x.dot(a.T))))
    y = 1 / (1+np.exp(-np.sum(x*a, 1, keepdims=True) ))
    return y

def grad_percent(a,x,y):
    '''
    INPUT:
    a - vector of weights 16x3
    x - vector of input data 18x16x3
    '''
    #return a * np.exp(-x.T.dot(a)) / (1+np.exp(-x.T.dot(a)))**2
    #return a*np.exp(-np.sum(x*a,1,keepdims=True)) / (1+np.exp(-np.sum(x*a,1,keepdims=True)))**2
    d0 = x.shape[0] if (len(x.shape) == 3) else 1
    
    a = np.repeat(a, d0, 0)
    x = x.reshape(-1, 3)
    y = y.reshape(-1, 1)
    y1 = -(2 * 
          ( y - 1/(1+np.exp(-np.sum(x.dot(a.T),1,keepdims=True))) ) * 
          1/(1+np.exp(-np.sum(x.dot(a.T),1,keepdims=True)))**2 *
          np.exp(-np.sum(x.dot(a.T),1,keepdims=True)) *
          x)
    
    return y1

def prepare_input(y, neigh_ndx):
    tmp_x = np.zeros((y.shape[0],3))
    for d in range(y.shape[0]):
        neigh = neigh_ndx[d]
        avg_n = [y[n,0]/np.sum(y[neigh,0]) for n in neigh]
        avg_n = sum(avg_n)/len(neigh)
        tmp_x[d] = np.array([y[d,0], avg_n, 1])
    return(tmp_x)

def model(a,x,Y,neigh_ndx):
    y = Y[0]
    loss = []
    out = np.zeros(Y.shape)
    out[0] = y
    for year in range(1,x.shape[0]):
        xi = prepare_input(y,neigh_ndx)
        y = model_percent(a,xi)
        loss.append(np.sum((y - Y[year])**2))
        #print(y.shape,'loss:', np.sum((y - Y[year])**2))
        out[year] = y
    return loss, out

def all_at_once(X, Y, i, q, neigh_ndx):
    arr_last = np.zeros((10,18))
    a_avg = np.random.rand(X.shape[1],X.shape[2])

    for epoch in range(10):
        grad = grad_percent(a_avg,X,Y).reshape(18,16,3)

        #if epoch==0: print('first grad max/min:', np.max(grad),'/',np.min(grad))
        grad = np.sum(grad, axis=0)
        
        #if epoch==0: print('first grad max/min:', np.max(grad),'/',np.min(grad))
        a_avg = a_avg - step*grad

        #if epoch%50==0: 
        #    if np.sum((model_percent(ap,X) - Y.reshape(-1,1))**2) < loss_p: step *= (1+beta)
        #    else: step /= (1-beta)

        loss_p = np.sum((model_percent(a_avg,X) - Y.reshape(-1,1))**2)

        #if epoch%1==0: print('loss sum:',loss_p)
        l, o = model(a_avg,X,Y,neigh_ndx)
        arr_last[epoch] = np.mean(o,1).reshape(-1)

    l, o = model(a_avg,X,Y,neigh_ndx)

    txt_to_be_saved = (
        str(i)
        +' '
        +np.array_str(arr_last).replace('\n',' ')
        +''
        +np.array_str(np.mean(o,1).reshape(-1)).replace('\n',' '))

    txt_to_be_saved = ' '.join(txt_to_be_saved.split())

    q.put(txt_to_be_saved)
    return txt_to_be_saved

def each_year_no_time(X, Y, i, q, neigh_ndx):
    arr_last = np.zeros((10,18))

    a_all = np.random.rand(X.shape[1],X.shape[2])
    loss_l = np.inf
    
    for epoch in range(10**3):
        shuffle_i = np.arange(X.shape[0])
        np.random.shuffle(shuffle_i)
        loss_p = 0
        for i in shuffle_i:
            grad = grad_percent(a_all,X[i],Y[i])#.reshape(18,16,3)
            #grad = np.sum(grad, axis=0)
            a_all = a_all - step*grad

            #if epoch%50==0: 
            #    if np.sum((model_percent(ap,X) - Y.reshape(-1,1))**2) < loss_p: step *= (1+beta)
            #    else: step /= (1-beta)

            loss_p += np.sum((model_percent(a_all,X[i]) - Y[i].reshape(-1,1))**2)

        if loss_p > loss_l: 
            #print('loss sum:',loss_p)
            break

        loss_l = loss_p

        if epoch%100==0: 
            #print('loss sum:',loss_p)
            n = epoch//(100)
            l, o = model(a_all,X,Y,neigh_ndx)
            arr_last[n] = np.mean(o,1).reshape(-1)

    l, o = model(a_all,X,Y,neigh_ndx)

    txt_to_be_saved = (
        str(i)
        +' '
        +np.array_str(arr_last).replace('\n',' ')
        +''
        +np.array_str(np.mean(o,1).reshape(-1)).replace('\n',' '))

    txt_to_be_saved = ' '.join(txt_to_be_saved.split())

    q.put(txt_to_be_saved)
    return txt_to_be_saved

def program_finall():
    # acctual program
    pool_data_middle, vote_list, neighbours, pool_d = prepare_data(True)
    X, Y = prepare_input_data(pool_data_middle,vote_list,neighbours,pool_d)

    neigh_ndx = []
    for d in range(X.shape[1]):
        # 1. last election: Blue, Red, Gray
        #    Blue/All
        # 2. neighbours
        # 3. one (1)
        lo = pool_d[0].iloc[d,:]        
        neigh = neighbours[lo.name.lower()]
        indexs = pool_d[0].index.values
        neigh_ndx.append(np.searchsorted(indexs, np.char.upper(neigh)))

    fun_list = [all_at_once, each_year_no_time]
    fil_list = ['all_at_once.txt', 'each_year_no_time.txt']
    for fun, fil in zip(fun_list,fil_list):
        # base file 
        bf = 'model/model1/'+fil 
        print(fil)
        save_file(fun, X, Y, neigh_ndx, fn=bf, iter=1000)

program_finall()