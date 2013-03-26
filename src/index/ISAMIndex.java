
package edu.buffalo.cse.sql.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.omg.CORBA.portable.RemarshalException;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Type;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.test.TestDataStream;

public class ISAMIndex implements IndexFile {

	private ManagedFile file;
	private IndexKeySpec keySpec;
	public ISAMIndex(ManagedFile file, IndexKeySpec keySpec)
	throws IOException, SqlException
	{
		this.file=file;
		this.keySpec=keySpec;
	}

	public static ISAMIndex create(FileManager fm,
			File path,
			Iterator<Datum[]> dataSource,
			IndexKeySpec key)
	throws SqlException, IOException
	{
		ManagedFile mf = fm.open(path);
		mf.resize(1);
		ByteBuffer b;
		DatumBuffer db=null;
		LinkedHashMap<Datum[],Integer> keyBufferMap = new LinkedHashMap<Datum[], Integer>();//<Index Key Datum, Page No>
		int countPages = 0;//Total no. of pages
		long countBytes=0;
		Datum[] keyDatum = null;

		int count=0;

		//First run for writing leaf pages: Data Level
		while(dataSource.hasNext())
		{
			count++;
			Datum tuple[] = dataSource.next();
			b = mf.getBuffer(countPages);
			db = new DatumBuffer(b,key.rowSchema());
			if(countBytes==0)
				db.initialize();
			try
			{
				mf.pin(countPages);
				db.write(tuple);
				mf.unpin(countPages,true);
				countBytes+=DatumSerialization.getLength(tuple);
			}
			catch(InsufficientSpaceException ise)
			{
				mf.unpin(countPages,true);//Required when db.write() throws Insufficient Space error
				keyDatum = key.createKey(tuple);
				keyBufferMap.put(keyDatum,countPages);//Storing tuples in page identified by countPage
				countPages++;
				countBytes=0;
				mf.resize(countPages+1);//We resize when we increase the no. of pages
				b = mf.getBuffer(countPages);
				db = new DatumBuffer(b, key.rowSchema());
				db.initialize();
				mf.pin(countPages);
				db.write(tuple);
				mf.unpin(countPages,true);
				countBytes+=DatumSerialization.getLength(tuple);
			}
		}
		createIndexPages(mf, countPages, keyBufferMap);
		fm.close(path);
		return null;
	}

	public static void printDatum(Datum[] a, int page)
	{
		System.out.print("Page:"+page+":");
		for(int i=0;i<a.length;i++)
		{
			System.out.print(a[i]+" ");
		}
	}

	public static void createIndexPages(ManagedFile mf,int countPages, LinkedHashMap<Datum[], Integer> keyBufferMap)throws SqlException, IOException
	{
		if(keyBufferMap.size()==0)
			return;
		else
		{
			//Second run for writing non-leaf pages: Index level
			Datum keyPointer[] = null;
			Datum[] keyPointerDatum = null;
			countPages++;//Start writing keys from next page
			mf.resize(countPages+1);
			int countBytes=0;
			LinkedHashMap<Datum[],Integer> keyBufferIndexMap = new LinkedHashMap<Datum[], Integer>();
			int count=0;
			for(Datum[] keyD:keyBufferMap.keySet())
			{
				count++;
				ByteBuffer b = mf.getBuffer(countPages);
				DatumBuffer db = new DatumBuffer(b, null);
				if(countBytes == 0)
					db.initialize();
				try{
					keyPointer = new Datum[keyD.length+2];
					keyPointer[0] = new Datum.Int(keyBufferMap.get(keyD));
					for(int i=0;i<keyD.length;i++)
						keyPointer[i+1] = keyD[i];
					keyPointer[keyPointer.length-1] = new Datum.Int(keyBufferMap.get(keyD)+1);

					mf.pin(countPages);
					if(countBytes==0)
					{
						Datum[] indicator=new Datum[1];
						indicator[0] = new Datum.Int(-2);
						db.write(indicator);
					}
					db.write(keyPointer);
					mf.unpin(countPages,true);
					countBytes+=DatumSerialization.getLength(keyPointer);
				}
				catch(InsufficientSpaceException ise1){
					mf.unpin(countPages,true);
					keyPointerDatum = keyPointer;
					Datum temp[]=new Datum[keyPointerDatum.length-2];
					for(int i=1;i<keyPointerDatum.length-1;i++)
						temp[i-1] = keyPointerDatum[i];
					keyBufferIndexMap.put(temp,countPages);//Storing tuples in page identified by countPage
					if(count < keyBufferMap.size()){
						countPages++;
						countBytes=0;
						mf.resize(countPages+1);
						b = mf.getBuffer(countPages);
						db = new DatumBuffer(b,null);
						db.initialize();
					}
				}
			}
			createIndexPages(mf, countPages, keyBufferIndexMap);
		}
	}

	public IndexIterator scan() 
	throws SqlException, IOException
	{
		/*For scan, we need to find currpage, currrecord, maxpage, maxrecord, 
		 * Intialize the indexKeySpec
		 */
		DatumStreamIterator dsi=new DatumStreamIterator(file, keySpec.rowSchema());
		dsi.key(keySpec);
		dsi.currPage(0);
		dsi.currRecord(0);

		Schema.Type[] keySchema =keySpec.keySchema();
		Schema.Type[] keyType = new Schema.Type[keySchema.length+2];
		keyType[0]=Type.INT;
		keyType[keyType.length-1]=Type.INT;
		for(int i=1;i<keyType.length-1;i++)
			keyType[i]=keySchema[i-1];

		Datum[] result = getMaxRecords(file.size()-1, keyType);
		dsi.maxPage(result[0].toInt());

		Datum[] maxRecord = new Datum[result.length-1];
		for(int i=1;i<result.length;i++)
			maxRecord[i-1] = result[i];
		dsi = dsi.ready();
		return dsi;
	}

	public IndexIterator rangeScanTo(Datum[] toKey)
	throws SqlException, IOException
	{
		DatumStreamIterator dsi=new DatumStreamIterator(file, keySpec.rowSchema());
		dsi.key(keySpec);
		dsi.currPage(0);
		dsi.currRecord(0);

		Schema.Type[] keySchema =keySpec.keySchema();
		Schema.Type[] keyType = new Schema.Type[keySchema.length+2];
		keyType[0]=Type.INT;
		keyType[keyType.length-1]=Type.INT;
		for(int i=1;i<keyType.length-1;i++)
			keyType[i]=keySchema[i-1];


		Datum[] result = getMaxRecords(file.size()-1, keyType,toKey);
		dsi.maxPage(result[0].toInt());
		Datum[] maxRecord = new Datum[result.length-1];
		for(int i=1;i<result.length;i++)
			maxRecord[i-1] = result[i];
		dsi.maxRecord(maxRecord);
		dsi=dsi.ready();
		return dsi;
	}

	public IndexIterator rangeScanFrom(Datum[] fromKey)
	throws SqlException, IOException
	{
		DatumStreamIterator dsi=new DatumStreamIterator(file, keySpec.rowSchema());
		dsi.key(keySpec);

		//Schema.Type[] keyType = {Type.INT,Type.INT,Type.INT,};
		Schema.Type[] keySchema =keySpec.keySchema();
		Schema.Type[] keyType = new Schema.Type[keySchema.length+2];
		keyType[0]=Type.INT;
		keyType[keyType.length-1]=Type.INT;
		for(int i=1;i<keyType.length-1;i++)
			keyType[i]=keySchema[i-1];

		Datum[] result = getFromRecord(file.size()-1, keyType,fromKey);
		dsi.currPage(result[0].toInt());
		dsi.currRecord(result[1].toInt());

		result = getMaxRecords(file.size()-1, keyType);
		dsi.maxPage(result[0].toInt());
		dsi = dsi.ready();
		return dsi;
	}

	public IndexIterator rangeScan(Datum[] start, Datum[] end)
	throws SqlException, IOException
	{
		DatumStreamIterator dsi=new DatumStreamIterator(file, keySpec.rowSchema());
		dsi.key(keySpec);

		Schema.Type[] keySchema =keySpec.keySchema();
		Schema.Type[] keyType = new Schema.Type[keySchema.length+2];
		keyType[0]=Type.INT;
		keyType[keyType.length-1]=Type.INT;
		for(int i=1;i<keyType.length-1;i++)
			keyType[i]=keySchema[i-1];

		Datum[] result = getFromRecord(file.size()-1, keyType,start);
		dsi.currPage(result[0].toInt());
		dsi.currRecord(result[1].toInt());

		result = getMaxRecords(file.size()-1, keyType,end);
		dsi.maxPage(result[0].toInt());
		Datum[] maxRecord = new Datum[result.length-1];
		for(int i=1;i<result.length;i++)
			maxRecord[i-1] = result[i];
		dsi.maxRecord(maxRecord);
		dsi=dsi.ready();
		return dsi;

	}

	public Datum[] get(Datum[] key)
	throws SqlException, IOException
	{
		Schema.Type[] keySchema =keySpec.keySchema();
		Schema.Type[] keyType = new Schema.Type[keySchema.length+2];
		keyType[0]=Type.INT;
		keyType[keyType.length-1]=Type.INT;
		for(int i=1;i<keyType.length-1;i++)
			keyType[i]=keySchema[i-1];

		Datum[] result = getDatumTuple(file.size()-1, keyType,key);
		return result;
	}

	public Datum[] getMaxRecords(int page, Schema.Type[] keyType) throws BufferException, IOException, CastError
	{
		ByteBuffer b = file.getBuffer(page);
		DatumBuffer db = new DatumBuffer(b, keyType);
		Datum[] keyPointer = db.read(0);
		if(keyPointer[0].equals(new Datum.Int(-2)))
		{
			keyPointer = db.read(db.length()-1);
			return getMaxRecords(keyPointer[keyPointer.length-1].toInt(), keyType);
		}
		else
		{
			keyType = new Schema.Type[keySpec.rowSchema().length];
			for(int i=0;i<keySpec.rowSchema().length;i++)
				keyType[i]=keySpec.rowSchema()[i];

			db = new DatumBuffer(b, keyType);
			Datum[] tuple = db.read(db.length()-1);
			Datum[] result = new Datum[tuple.length+1];
			result[0]=new Datum.Int(page);
			for(int i=0;i<tuple.length;i++)
				result[i+1] = tuple[i];
			return result;
		}
	}

	public Datum[] getMaxRecords(int page, Schema.Type[] keyType, Datum[] toKey) throws BufferException, IOException, CastError
	{
		ByteBuffer b = file.getBuffer(page);
		DatumBuffer db = new DatumBuffer(b, keyType);
		Datum[] keyPointer = db.read(0);
		if(keyPointer[0].equals(new Datum.Int(-2)))
		{
			int nextPage=-1;
			for(int i=1;i<db.length();i++)
			{
				keyPointer = db.read(i);
				Datum[] keyToBeCompared = new Datum[keyPointer.length-2];
				for(int j=0;j<keyToBeCompared.length;j++)
					keyToBeCompared[j] = keyPointer[j+1];
				if(keySpec.compare(toKey,keyToBeCompared)<0)
				{
					nextPage = keyPointer[0].toInt();
					break;
				}
				else if(keySpec.compare(toKey,keyToBeCompared)>=0)
					nextPage = keyPointer[keyPointer.length-1].toInt();
			}
			return getMaxRecords(nextPage, keyType,toKey);
		}
		else
		{
			keyType = new Schema.Type[keySpec.rowSchema().length];
			for(int i=0;i<keySpec.rowSchema().length;i++)
				keyType[i]=keySpec.rowSchema()[i];

			db = new DatumBuffer(b, keyType);
			for(int i=0;i<db.length();i++)
			{
				Datum[] tuple = db.read(i);
				Datum[] keyComp = keySpec.createKey(tuple);
				if(keySpec.compare(toKey,keyComp)<0)
				{
					Datum[] result=new Datum[tuple.length+1];
					result[0]=new Datum.Int(page);
					for(int j=1;j<result.length;j++)
						result[j]=tuple[j-1];
					return result;
				}
				else if(keySpec.compare(toKey,keyComp)==0 && i==db.length()-1){
					ByteBuffer b1 = file.getBuffer(page+1);
					DatumBuffer db1 = new DatumBuffer(b1, keyType);
					Datum[] tuple1=db1.read(0);
					if(tuple1[0].toInt()!=-2)
					{
						Datum[] result=new Datum[tuple1.length+1];
						result[0]=new Datum.Int(page+1);
						for(int j=1;j<result.length;j++)
							result[j]=tuple1[j-1];
						return result;
					}
					else{
						Datum[] result=new Datum[tuple.length+1];
						result[0]=new Datum.Int(page);
						for(int j=1;j<result.length;j++)
							result[j]=tuple[j-1];
						return result;
					}
				}
			}
		}
		return null;
	}

	public Datum[] getFromRecord(int page, Schema.Type[] keyType, Datum[] fromKey) throws BufferException, IOException, CastError
	{
		ByteBuffer b = file.getBuffer(page);
		DatumBuffer db = new DatumBuffer(b, keyType);
		Datum[] keyPointer = db.read(0);
		if(keyPointer[0].equals(new Datum.Int(-2)))
		{
			int nextPage=-1;
			for(int i=1;i<db.length();i++)
			{
				keyPointer = db.read(i);
				Datum[] keyToBeCompared = new Datum[keyPointer.length-2];
				for(int j=0;j<keyToBeCompared.length;j++)
					keyToBeCompared[j] = keyPointer[j+1];
				if(keySpec.compare(fromKey,keyToBeCompared)<0)
				{
					nextPage = keyPointer[0].toInt();
					break;
				}
				else if(keySpec.compare(fromKey,keyToBeCompared)>=0)
					nextPage = keyPointer[keyPointer.length-1].toInt();
			}
			return getFromRecord(nextPage, keyType,fromKey);
		}
		else
		{
			keyType = new Schema.Type[keySpec.rowSchema().length];
			for(int i=0;i<keySpec.rowSchema().length;i++)
				keyType[i]=keySpec.rowSchema()[i];

			db = new DatumBuffer(b, keyType);
			for(int i=0;i<db.length();i++)
			{
				Datum[] tuple = db.read(i);
				Datum[] keyComp = keySpec.createKey(tuple);
				if(keySpec.compare(fromKey,keyComp)<=0)
				{
					Datum[] result=new Datum[2];
					result[0] = new Datum.Int(page);
					result[1] = new Datum.Int(i);
					return result;
				}
			}
		}
		return null;
	}

	public Datum[] getDatumTuple(int page,Schema.Type[] keyType,Datum[] key) throws BufferException, IOException, CastError
	{
		ByteBuffer b;
		b = file.getBuffer(page);
		DatumBuffer db = new DatumBuffer(b,keyType);
		Datum[] keyPointer = db.read(0);
		int nextPage = -1;
		if(keyPointer[0].equals(new Datum.Int(-2)))
		{
			for(int i=1;i<db.length();i++)
			{
				keyPointer = db.read(i);
				Datum[] keyToBeCompared = new Datum[keyPointer.length-2];
				for(int j=0;j<keyToBeCompared.length;j++)
					keyToBeCompared[j] = keyPointer[j+1];

				if(keySpec.compare(key,keyToBeCompared)<0)
				{
					nextPage = keyPointer[0].toInt();
					break;
				}
				else if(keySpec.compare(key,keyToBeCompared)>=0)
					nextPage = keyPointer[keyPointer.length-1].toInt();
			}
			return getDatumTuple(nextPage, keyType,key);
		}
		else
		{

			keyType=new Schema.Type[keySpec.rowSchema().length];
			for(int i=0;i<keySpec.rowSchema().length;i++)
				keyType[i]=keySpec.rowSchema()[i];

			db = new DatumBuffer(b, keyType);
			for(int i=0;i<db.length();i++)
			{
				Datum[] tuple = db.read(i);
				Datum[] keyComp = keySpec.createKey(tuple);
				if(keySpec.compare(key,keyComp)==0)
					return tuple;
			}
		}
		return null;
	}

}