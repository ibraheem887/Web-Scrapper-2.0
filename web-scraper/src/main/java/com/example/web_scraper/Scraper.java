package com.example.web_scraper;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.*;

public class Scraper {
    private static final String BASE_URL = "https://papers.nips.cc";
    private static final String DESKTOP_PATH = "E:/NeurIPS_Papers/";

    public static void main(String[] args) {
        // Create base folder on Desktop
        try {
            Files.createDirectories(Paths.get(DESKTOP_PATH));
        } catch (IOException e) {
            System.err.println("Failed to create NeurIPS_Papers folder on Desktop");
            e.printStackTrace();
            return;
        }

        // Executor service to manage multiple threads for paper processing and downloading
        ExecutorService executorService = Executors.newFixedThreadPool(50); // Limit to 50 threads for paper processing

        // CSV file header
        try (FileWriter writer = new FileWriter(DESKTOP_PATH + "output.csv")) {
            writer.append("Year,Title,Authors,Paper Link,PDF Link\n");

            try {
                // Process the years you need; here, for example, 2023 to 2019
                for (int year = 2023; year >= 2019; year--) {
                    String yearUrl = BASE_URL + "/paper_files/paper/" + year;
                    String yearFolder = DESKTOP_PATH + year + "/";

                    // Create a folder for each year
                    Files.createDirectories(Paths.get(yearFolder));

                    System.out.println("Fetching papers from: " + yearUrl);
                    Document yearDoc;
                    try {
                        yearDoc = Jsoup.connect(yearUrl).get();
                    } catch (IOException e) {
                        System.err.println("Failed to fetch URL: " + yearUrl);
                        e.printStackTrace();
                        continue;
                    }

                    // Use the new selector: ul.paper-list li a
                    Elements paperLinks = yearDoc.select("ul.paper-list li a");
                    if (paperLinks.isEmpty()) {
                        System.err.println("No paper links found on page: " + yearUrl);
                        continue;
                    }

                    // Submit the scraping task to the executor
                    CountDownLatch latch = new CountDownLatch(paperLinks.size());
                    for (Element paperLink : paperLinks) {
                        final String paperTitle = paperLink.text().trim();  // Declare as final
                        final String paperPageUrl = BASE_URL + paperLink.attr("href");  // Declare as final
                        final int currentYear = year;  // Use a final local variable for year
                        final String yearFolderPath = yearFolder;  // Make yearFolder final as well

                        // Submit the scraping task to the executor using an anonymous Runnable
                        executorService.submit(new Runnable() {
                            @Override
                            public void run() {
                                scrapePaper(paperTitle, paperPageUrl, currentYear, yearFolderPath, writer, latch);
                            }
                        });
                    }
                    latch.await();  // Wait for all tasks to complete before moving to the next year
                    System.out.println("Completed processing year: " + year);
                }
            } catch (IOException | InterruptedException e) {
                System.err.println("An error occurred during scraping.");
                e.printStackTrace();
            } finally {
                executorService.shutdown();
            }

            System.out.println("Data successfully written to output.csv.");
        } catch (IOException e) {
            System.err.println("Error opening the CSV file for writing.");
            e.printStackTrace();
        }
    }

    // Function that scrapes a single paper's data and writes to CSV
    private static void scrapePaper(String paperTitle, String paperPageUrl, int year, String yearFolder, FileWriter writer, CountDownLatch latch) {
        int attempt = 1;  // Track the number of attempts
        while (attempt <= 3) {  // Retry up to 3 times
            try {
                Document paperDoc = Jsoup.connect(paperPageUrl).timeout(60000).get();

                // Extract authors from <i> tags.
                Elements authorElements = paperDoc.select("i");
                StringBuilder authorNames = new StringBuilder();
                if (authorElements.isEmpty()) {
                    System.out.println("No authors found for: " + paperTitle);
                } else {
                    for (Element author : authorElements) {
                        authorNames.append(author.text()).append(" ");
                    }
                }

                // Find the PDF download link (assume it's the first link ending with .pdf)
                Element pdfElement = paperDoc.selectFirst("a[href$=.pdf]");
                String pdfUrl = (pdfElement != null) ? BASE_URL + pdfElement.attr("href") : "N/A";

                // Download PDF if available
                if (!pdfUrl.equals("N/A")) {
                    String sanitizedTitle = paperTitle.replaceAll("[^a-zA-Z0-9]", "_") + ".pdf";
                    String savePath = yearFolder + sanitizedTitle;
                    System.out.println("Downloading PDF: " + pdfUrl);
                    downloadFile(pdfUrl, savePath);
                } else {
                    System.out.println("No PDF link found for: " + paperTitle);
                }

                // Write the paper data directly to the CSV file
                synchronized (writer) {
                    writer.append(String.format("%d,\"%s\",\"%s\",\"%s\",\"%s\"\n",
                            year,
                            paperTitle,
                            authorNames.toString(),
                            paperPageUrl,
                            pdfUrl));
                    writer.flush();  // Ensure the data is written to the file immediately
                }

                break;  // Exit the loop after a successful task
            } catch (IOException e) {
                System.err.println("Error processing paper: " + paperTitle + " (Attempt " + attempt + " of 3)");
                e.printStackTrace();
                if (attempt < 3) {
                    // Wait before retrying (e.g., 10 seconds between attempts)
                    try {
                        Thread.sleep(10000);  // Retry after 10 seconds
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    attempt++;
                    System.out.println("Retrying paper: " + paperTitle + " (" + paperPageUrl + ")");
                } else {
                    // After 3 attempts, log failure and move on
                    System.err.println("Failed to process paper after 3 attempts: " + paperTitle);
                    break;
                }
            } finally {
                latch.countDown();  // Decrease the latch count when the task is done
            }
        }
    }

    // Function to download the PDF file
    private static void downloadFile(String fileURL, String savePath) {
        try (InputStream in = new URL(fileURL).openStream();
             FileOutputStream out = new FileOutputStream(savePath)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            System.out.println("Downloaded: " + savePath);
        } catch (IOException e) {
            System.err.println("Failed to download: " + fileURL);
            e.printStackTrace();
        }
    }
}
