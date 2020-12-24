package me.channa.jmstool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import me.channa.jmstool.commands.ConsumeCommand;
import me.channa.jmstool.commands.HelpCommand;
import me.channa.jmstool.commands.PublishCommand;
import org.apache.log4j.*;

public class JmsTool {

    @Parameter(names = "-v", description = "Verbose mode logging")
    private boolean verbose = false;

    private static final Logger LOGGER = Logger.getLogger(JmsTool.class);

    public static final String PUBLISH_COMMAND = "publish";
    public static final String CONSUME_COMMAND = "consume";
    public static final String HELP_COMMAND = "help";

    public static void main(String[] args) {
        // Initiate logging
        ConsoleAppender consoleAppender = new ConsoleAppender(new PatternLayout("%d [%t] %p %c %x - %m%n"));
        BasicConfigurator.configure(consoleAppender);
        LOGGER.info("JmsTool 1.1 - Please visit https://github.com/hjchanna/jms-tool for more information");

        // Initiate commands and parse args
        JmsTool jmsTool = new JmsTool();
        PublishCommand publishCommand = new PublishCommand();
        ConsumeCommand consumeCommand = new ConsumeCommand();
        HelpCommand helpCommand = new HelpCommand();

        JCommander jCommander = JCommander.newBuilder()
                .addObject(jmsTool)
                .addCommand(PUBLISH_COMMAND, publishCommand)
                .addCommand(CONSUME_COMMAND, consumeCommand)
                .addCommand(HELP_COMMAND, helpCommand)
                .build();

        jCommander.parse(args);

        // Set logging level
        if (jmsTool.verbose) {
            Logger.getRootLogger().setLevel(Level.DEBUG);
        } else {
            Logger.getRootLogger().setLevel(Level.INFO);
        }

        // run command
        String command = jCommander.getParsedCommand();

        if (command == null) {
            LOGGER.error("Invalid arguments, please send correct arguments");
            helpCommand.execute(jCommander);
        }

        switch (command) {
            case PUBLISH_COMMAND:
                publishCommand.execute(jCommander);
                break;
            case CONSUME_COMMAND:
                consumeCommand.execute(jCommander);
                break;
            case HELP_COMMAND:
                helpCommand.execute(jCommander);
                break;
            default:
                LOGGER.error(String.format("Operation %s not supported, please send correct arguments", command));
                helpCommand.execute(jCommander);
                break;
        }
    }
}
