
# coding: utf-8

# In[1]:

import numpy as np
import matplotlib.pyplot as plt
#%matplotlib inline


# In[2]:

def write_query(ix, iy, iz, n_web=256, n_size=1024):
    ratio = n_size/n_web
    query=" select * from Bolshoi.Tweb%d m where floor(m.ix) = %d"%(n_web, np.floor(ix/ratio))
    query= query+" and floor(m.iy) = %d"%(np.floor(iy/ratio))
    query= query+" and floor(m.iz) = %d"%(np.floor(iz/ratio))
    return query


# In[3]:

def get_web_data(cosmosim, sample_name="sat"):
    #this defines the range of the ix, iy, iz integers in the simulation
    n_bits = 10
    n_size = 2**n_bits
    lbox = 250.0
    n_web = 256

    sample_filename = "../data/samples/sample%s.txt"%(sample_name)
    tweb_filename="../data/tweb/web_%d_sample%s.dat"%(n_web, sample_name)
    
    #load sample data
    halo_data = np.loadtxt(sample_filename)
    x_halo = halo_data[:,0]
    y_halo = halo_data[:,1]
    z_halo = halo_data[:,2]

    ix = np.int_((x_halo/lbox)*n_size)
    iy = np.int_((y_halo/lbox)*n_size)
    iz = np.int_((z_halo/lbox)*n_size)
    print np.size(x_halo), 256**3

    
    # Submit, get, write
    n_points = np.size(x_halo)
    n_points = 4
    jobs = np.zeros(n_points, dtype='int')
    print("n_points in total %d\n"%(n_points))
    for i in range(n_points):
        #submit job
        query = write_query(ix[i], iy[i], iz[i], n_web=n_web, n_size=n_size)
        jobs[i] = cosmosim.run_sql_query(query_string=query)
        print("item %d out of %d"%(i, n_points))

        # Download data        
        headers, data = cosmosim.download(jobid=jobs[i],format='csv')

        #write header in first item
        if(i==0):
            fileout = open(tweb_filename, 'w')
            string = "# ID"
            for item in headers:
                string = string + " "+item+" "
            fileout.write(" %s\n"%(string))
            fileout.close()

        #write data line
        fileout = open(tweb_filename, 'a')
        string = "%d "%(i)
        for item in data[0]:
            string = string + " "+str(item)+" "
        fileout.write(" %s\n"%(string))
        fileout.close()

        
        #delete job
        #cosmosim.delete_job(jobid=jobs[i])


from astroquery.cosmosim import CosmoSim
CS = CosmoSim()
CS.login(username="forero",store_password=True)
CS.check_login_status()
get_web_data(CS, sample_name="sat")


# In[ ]:



