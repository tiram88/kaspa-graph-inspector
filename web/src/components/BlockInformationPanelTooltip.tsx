import {ReactChild, ReactElement, ReactFragment, ReactPortal} from "react";
import {makeStyles, Tooltip} from "@material-ui/core";

const useStyles = makeStyles(() => ({
    tooltip: {
        maxWidth: "none",
        backgroundColor: "#175676",
        border: "1px solid #ffffff",
        fontSize: "0.9rem",
    },
    arrow: {
        "&::before": {
            backgroundColor: "#175676",
            border: "1px solid #ffffff",
        },
    }
}));

const BlockInformationPanelTooltip = ({children, title}:
                                          { children: ReactElement, title: boolean | ReactChild | ReactFragment | ReactPortal }) => {

    const classes = useStyles();

    return <Tooltip title={title} arrow interactive placement="right"
                    classes={{tooltip: classes.tooltip, arrow: classes.arrow}}>
        {children}
    </Tooltip>
};

export default BlockInformationPanelTooltip;
