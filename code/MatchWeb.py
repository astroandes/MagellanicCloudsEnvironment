
# coding: utf-8

# In[1]:

import numpy as np
import matplotlib.pyplot as plt
#%matplotlib inline


# In[2]:

def write_query(ix, iy, iz, n_web=256, n_size=1024):
    ratio = n_size/n_web
    query=" select * from Bolshoi.Tweb%d m where floor(m.ix) = %d"%(n_web, floor(ix/ratio))
    query= query+" and floor(m.iy) = %d"%(floor(iy/ratio))
    query= query+" and floor(m.iz) = %d"%(floor(iz/ratio))
    return query


# In[3]:

def get_web_data(cosmosim, sample_name="sat"):
    #this defines the range of the ix, iy, iz integers in the simulation
    n_bits = 10
    n_size = 2**n_bits
    lbox = 250.0

    sample_filename = "../data/samples/sample%s.txt"%(sample_name)
    tweb_filename="../data/tweb/web_%d_sample%s.dat"%(n_size, sample_name)
    
    #load sample data
    halo_data = np.loadtxt(sample_filename)
    x_halo = halo_data[:,0]
    y_halo = halo_data[:,1]
    z_halo = halo_data[:,2]

    ix = int_((x_halo/lbox)*n_size)
    iy = int_((y_halo/lbox)*n_size)
    iz = int_((z_halo/lbox)*n_size)
    print np.size(x_halo), 256**3
    
    # Submit jobs
    n_points = np.size(x_halo)
    jobs = np.zeros(n_points, dtype='int')
    n_points = 2
    for i in range(n_points):
        query = write_query(ix[i], iy[i], iz[i], n_web=256, n_size=1024)
        jobs[i] = cosmosim.run_sql_query(query_string=query)
        print(jobs[i])
        
    # Download data
    all_data = []
    for i in range(n_points):
        headers, data = cosmosim.download(jobid=jobs[i],format='csv')
        print data
        all_data.append(data[0])
        
    # Write the data to disk
    fileout = open(tweb_filename, 'w')

    string = "# ID"
    for item in headers:
        string = string + " "+item+" "
    fileout.write(" %s\n"%(string))

    n_lines = len(all_data)
    for i in range(n_lines):
        string = "%d "%(i)
        for item in all_data[i]:
            string = string + " "+str(item)+" "
        fileout.write("%s\n"%(string))
    fileout.close()


# In[4]:

from astroquery.cosmosim import CosmoSim
CS = CosmoSim()
CS.login(username="forero",store_password=True)
CS.check_login_status()
get_web_data(CS, sample_name="sat")


# In[ ]:



