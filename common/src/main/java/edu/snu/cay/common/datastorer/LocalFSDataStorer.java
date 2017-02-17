package edu.snu.cay.common.datastorer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by yunseong on 2/13/17.
 */
public class LocalFSDataStorer implements DataStorer {

  @Override
  public void storeData(final String path, final byte[] data) {
    final Path p = new Path(path);
    final FileSystem fs = new LocalFileSystem();

    try (final FSDataOutputStream fos = fs.create(new Path("/tmp/yunseong"))){
      fos.write(data);

      fos.close();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      System.out.println("No problem!");
    }
  }

  public static void main(String[] args) {
    final LocalFSDataStorer storer = new LocalFSDataStorer();
    storer.storeData("/tmp", "a".getBytes());
  }
}
