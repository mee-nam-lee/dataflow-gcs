// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.examples.dataflow.batch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

// [START pubsub_to_gcs]

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

public class ZipGCStoGCS {
  /*
   * Define your own configuration options. Add your own arguments to be processed
   * by the command-line parser, and specify default values for them.
   */
  public interface GcsToGcsOptions extends PipelineOptions, GcpOptions {
    @Description("The Cloud Pub/Sub topic to read from.")
    @Required
    String getInputBucket();

    void setInputBucket(String value);
    
    @Description("The Cloud Pub/Sub topic to read from.")
    @Required
    String getOutputBucket();

    void setOutputBucket(String value);

    @Description("Output file's window size in number of minutes.")
    @Default.Integer(1)
    Integer getWindowSize();

    void setWindowSize(Integer value);

    @Description("Path of the output file including its filename prefix.")
    @Required
    String getOutput();

    void setOutput(String value);
    
  }
  
  static class Zipper extends DoFn<KV<String, Iterable<String>>, String> {
	    private static final Logger LOG = LoggerFactory.getLogger(Zipper.class);
	    private static Storage storage;
	    private String input_bucket;
	    private String output_bucket;
	    private Integer window_size;
	    
	    private Zipper(String project_id, String input_bucket, String output_bucket, Integer window_size) {
	    	Zipper.storage = StorageOptions.newBuilder().setProjectId(project_id).build().getService();;
	    	this.input_bucket = input_bucket;
	    	this.output_bucket = output_bucket;
	    	this.window_size = window_size;
	    	
	    }
	    @ProcessElement
	    public void processElement(ProcessContext c) {
		  System.out.println("====Zipper Process Element ====" + c.element());
      	  KV<String, Iterable<String>> e = c.element();
      	  ArrayList<String> in_images = (ArrayList<String>) e.getValue();
			  System.out.println("========" + e.getKey());
			  System.out.println(e.getValue());
	//		  System.out.println("size   " + in_images.size());
			 
		        try {
		        	String zipname = in_images.get(0).split("/")[1] + "_win_" + this.window_size +".zip";
					FileOutputStream fos = new FileOutputStream(zipname);
					System.out.println("=======" + zipname);
					ZipOutputStream zipOut = new ZipOutputStream(fos);
	//				File fileToZip = new File(sourceFile);
	//				FileInputStream fis = new FileInputStream(fileToZip);
					
					
					for (int i = 0; i < in_images.size(); i++) {
	//					System.out.println("=======" + in_images.get(i));
						Page<Blob> blobs = Zipper.storage.list(this.input_bucket, 
									                Storage.BlobListOption.currentDirectory(), 
									                Storage.BlobListOption.prefix(in_images.get(i)));
						
						  for (Blob blob : blobs.iterateAll()) { 
//							  System.out.println(blob.getContent());
							  zipOut.putNextEntry(new ZipEntry(blob.getName()));
							  zipOut.write(blob.getContent());							  
						  }
					}
					
					
	//				ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
	//				zipOut.putNextEntry(zipEntry);
					
					
	/*
	 * byte[] bytes = new byte[1024]; int length; while((length = fis.read(bytes))
	 * >= 0) { zipOut.write(bytes, 0, length); }
	 */
					
					
					zipOut.close();
	//				fis.close();
					fos.close();
				    BlobId blobId = BlobId.of(this.output_bucket, zipname);
				    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
					Zipper.storage.create(blobInfo, Files.readAllBytes(Paths.get(zipname)));
					
					File file = new File(zipname);
					file.delete();
					
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

	      
	    }
  }
  
  
  

  public static List<KV<String,String>> files(String input_bucket,  String project_id){
  //  String path = input_bucket + "/200/img";
    
	List<KV<String,String>> filelist = new ArrayList<KV<String,String>>();
    Storage storage = StorageOptions.newBuilder().setProjectId(project_id).build().getService();
    Page<Blob> blobs = storage.list(input_bucket, 
    		                Storage.BlobListOption.currentDirectory(), 
    		                Storage.BlobListOption.prefix("200/img"));
    
	KV<String,String> file = null;

	  for (Blob blob : blobs.iterateAll()) { 
//		  System.out.println(blob.getName());
	      filelist.add(KV.of("key",blob.getName())); 
	  }
	 
//	System.out.println(filelist);
    return filelist;

  }

public static void main(String[] args) throws IOException {
    // The maximum number of shards when writing output.
 //   int numShards = 1;

    GcsToGcsOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(GcsToGcsOptions.class);
    
  //  Storage storage = StorageOptions.newBuilder().setProjectId(options.getProject()).build().getService();

 //   options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);
	System.out.println("................" + options.getWindowSize());

    pipeline
          .apply("Test", Create.of(files(options.getInputBucket(), options.getProject())))
  //        .apply("print", MapElements.into(TypeDescriptors.lists(null)).via(s ->  {System.out.println(s.getValue()); return s;}))
          .apply("Group",GroupIntoBatches.ofSize(options.getWindowSize()))
          .apply("Zip", ParDo.of(new Zipper(options.getProject(), options.getInputBucket(), options.getOutputBucket(), options.getWindowSize())));

    
 //         .apply("Test", TextIO.read().from("gs://mnlee-tsop/1.txt")); 
 //       .apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
        // 2) Group the messages into fixed-sized minute intervals.
 //       .apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
        // 3) Write one file to GCS for every window of messages.
 //       .apply("Write Files to GCS", new WriteOneFilePerWindow(options.getOutput(), numShards));

    // Execute the pipeline and wait until it finishes running.
    pipeline.run().waitUntilFinish();
  //  pipeline.run();
  }
}
// [END pubsub_to_gcs]
