package edu.buffalo.cse.sql.index;

import java.util.List;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.test.TestDataStream;

public class HashIndex implements IndexFile {
	ManagedFile mangfile = null;
	IndexKeySpec inkeyspec = null;

	public HashIndex(ManagedFile file, IndexKeySpec keySpec)
	throws IOException, SqlException {

		//System.out.println("Inside HashIndex constructor");
		//System.out.println(file + " " + keySpec);
		mangfile = file;
		inkeyspec = keySpec;
	}


	public static HashIndex create(FileManager fm, File path,
			Iterator<Datum[]> dataSource, IndexKeySpec key, int directorySize)
	throws SqlException, IOException {

		ManagedFile mf = fm.open(path); 
		mf.ensureSize(directorySize); 
		int indexofbucket = -1;
		//System.out.println("Path= "+ path.getAbsolutePath());
		DatumBuffer d = null;
		for (int i = 0; i < mf.size(); i++) 
		{
			ByteBuffer b = mf.safePin(i);
			d = new DatumBuffer(b, key.rowSchema());
			d.initialize(8);
			Datum[] put = new Datum[2];
			put[0] = new Datum.Int(-1);
			put[1] = new Datum.Int(directorySize);
			DatumSerialization.write(b, 0, put[0]);
			DatumSerialization.write(b, 4, put[1]);

			mf.unpin(i, true);

		}

		while (dataSource.hasNext()) 
		{
			Datum[] tuple = dataSource.next(); 
			indexofbucket = key.hashRow(tuple) % directorySize; 
			ByteBuffer b = mf.safePin(indexofbucket);
			d = new DatumBuffer(b, key.rowSchema());

			try {

				d.write(tuple);
				mf.unpin(indexofbucket, true);
			} catch (Exception e) 
			{

				int temp = 0;
				int overflowind = (DatumSerialization.read(b, 0, 
						Schema.Type.INT)).toInt();
				mf.unpin(indexofbucket, true);
				if (overflowind == -1) {
					mf.pin(indexofbucket);

					DatumSerialization.write(b, 0, new Datum.Int(mf.size()));
					mf.unpin(indexofbucket, true);
					mf.ensureSize(mf.size() + 1); 
					b = mf.safePin(mf.size() - 1);
					d = new DatumBuffer(b, key.rowSchema());
					d.initialize(8); 
					Datum[] put = new Datum[2];
					put[0] = new Datum.Int(-1);
					put[1] = new Datum.Int(directorySize);
					DatumSerialization.write(b,0,put[0]);
					DatumSerialization.write(b,0,put[1]);
					d.write(tuple);
					mf.unpin(mf.size() - 1, true);

				}

				else {

					int writeflag = 0;
					int oldval = 0;

					while (overflowind != -1 ) {

						b = mf.getBuffer(overflowind);
						mf.pin(overflowind);
						temp = overflowind;
						d = new DatumBuffer(b, key.rowSchema());
						if ((d.remaining() -8) - (DatumSerialization.getLength(tuple)) > 0) 

						{
							try
							{		
								d.write(tuple);
								mf.unpin(overflowind, true);
								writeflag = 1;
								break;
							}
							catch(Exception e1)
							{
								mf.unpin(overflowind, true);

							}
						}

						else 
						{
							oldval = overflowind;
							mf.unpin(temp, true);
							overflowind = (DatumSerialization.read(b, 0,
									Schema.Type.INT)).toInt();
							if(overflowind == -1)
							{

								b = mf.safePin(oldval);
								if ((d.remaining() -8) - (DatumSerialization.getLength(tuple)) > 0)
								{
									d.write(tuple);
									mf.unpin(oldval, true);
									writeflag = 1;	
									break;
								}
							}

						}


					}
					if (overflowind == -1 && writeflag == 0) {
						mf.unpin(temp, true);
						DatumSerialization.write(b, 0, new Datum.Int(mf.size()));
						mf.ensureSize(mf.size() + 1); 

						b = mf.safePin(mf.size() - 1);
						d = new DatumBuffer(b, key.rowSchema());
						d.initialize(8); 
						Datum[] put = new Datum[2];
						put[0] = new Datum.Int(-1);
						put[1] = new Datum.Int(directorySize);
						DatumSerialization.write(b, 0, put[0]);
						DatumSerialization.write(b, 4, put[1]);

						d.write(tuple);
						mf.unpin(mf.size() - 1, true);
					}

				}
			}
		}
		fm.close(path);
		return null;

	}

	public IndexIterator scan() throws SqlException, IOException {
		throw new SqlException("Unimplemented");
	}

	public IndexIterator rangeScanTo(Datum[] toKey) throws SqlException,
	IOException {
		throw new SqlException("Unimplemented");
	}

	public IndexIterator rangeScanFrom(Datum[] fromKey) throws SqlException,
	IOException {
		throw new SqlException("Unimplemented");
	}

	public IndexIterator rangeScan(Datum[] start, Datum[] end)
	throws SqlException, IOException {
		throw new SqlException("Unimplemented");
	}

	public Datum[] get(Datum[] key) throws SqlException, IOException {

		ByteBuffer b = mangfile.getBuffer(0);
		int directorysize = DatumSerialization.read(b, 4, inkeyspec.rowSchema())[0].toInt();
		int rownumber = inkeyspec.hashKey(key) % directorysize;// page number
		b = mangfile.getBuffer(rownumber);
		int oldval = 0;
		DatumBuffer d = new DatumBuffer(b, inkeyspec.rowSchema());
		int position = d.find(key);
		Datum[] tuple = d.read(position);
		Datum[] somekey = inkeyspec.createKey(tuple);
		if (inkeyspec.compare(somekey, key) == 0) // frontend buckets with the key, if key matches
		{
			return d.read(d.find(key));
		}

		else // if key not matched then go to overflow page
		{
			int overflowind = DatumSerialization.read(b,0,Schema.Type.INT).toInt();

			if (overflowind != -1) {
				while(true)
				{
					try{
						b = mangfile.getBuffer(overflowind);
						d = new DatumBuffer(b, inkeyspec.rowSchema());
						position = d.find(key);// inside this buffer, check whether key is present or not
						tuple = d.read(position);
						somekey = inkeyspec.createKey(tuple);
						if (inkeyspec.compare(somekey, key) == 0) // frontend buckets with the key, if key matches
						{
							return d.read(d.find(key));
						}
						oldval = overflowind;
						overflowind = (DatumSerialization.read(b, 0,inkeyspec.rowSchema()))[0].toInt();
					}//end of try
					catch(Exception e2)
					{
						break;
					}
				}
			}//end of if (overflowind != -1)
		}
		//for page with -1
		b = mangfile.getBuffer(oldval);
		d = new DatumBuffer(b, inkeyspec.rowSchema());
		position = d.find(key);// inside this buffer, check whether key is present or not
		tuple = d.read(position);
		somekey = inkeyspec.createKey(tuple);
		if (inkeyspec.compare(somekey, key) == 0) // frontend buckets with the key, if key matches
		{
			return d.read(d.find(key));
		}
		return null;
	}
}