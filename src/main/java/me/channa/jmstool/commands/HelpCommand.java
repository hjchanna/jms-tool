package me.channa.jmstool.commands;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

@Parameters
public class HelpCommand implements ICommand {

    @Override
    public void execute(JCommander jCommander) {
        jCommander.usage();
    }
}
