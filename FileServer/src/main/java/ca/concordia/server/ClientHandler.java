package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;
import java.net.Socket;
import java.io.*;//To be able to import all classes of java.io

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final FileSystemManager fsManager;

    public ClientHandler(Socket clientSocket, FileSystemManager fsManager) {
        this.clientSocket = clientSocket;
        this.fsManager = fsManager;
    }

    public void run() {
        //Error handling
        try(BufferedReader reader=new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        PrintWriter writer=new PrintWriter(clientSocket.getOutputStream(),true))
        {
            String line;
            while((line=reader.readLine()) != null){
                System.out.println("The Client sent the request: "+line);
                String[] parts=line.split(" ",3);
                String command=parts[0].toUpperCase();

                try {
                    switch (command) {
                        case "CREATE":
                            fsManager.createFile(parts[1]);
                            writer.println("File: "+parts[1]+" created!");
                            break;

                        case "WRITE":
                            fsManager.writeFile(parts[1],parts[2].getBytes());
                            writer.println("The content was written into the file!");
                            break;

                        case "READ":
                            byte[] data=fsManager.readFile(parts[1]);
                            writer.println(new String(data));
                            break;

                        case "DELETE":
                            fsManager.deleteFile(parts[1]);
                            writer.println("The file: "+parts[1]+" has been deleted!");
                            break;

                        case "LIST":
                            String files=fsManager.listFiles();
                            writer.println(String.join(",",files));
                            break;

                        default:
                            writer.println("ERROR:Unknown command");
                    }
                }

                catch (Exception e){
                    writer.println(e.getMessage());
                }
            }
        }

        catch (Exception e){
            System.out.println("The Client got disconnected...");
        }
    }
}
